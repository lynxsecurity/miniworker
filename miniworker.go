package miniworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"

	"github.com/lynxsecurity/minimsg"
	"github.com/lynxsecurity/tinylog"
)

// MiniWorker defines the interface for the microservice worker.
type MiniWorker interface {
}

type Job struct {
	Action string
	Domain string
	Range  string
}

type runnerFunc func(log *tinylog.Tiny, job Job) error

// Worker is the actual implementation
type Worker struct {
	hostname   string
	log        *tinylog.Tiny
	threads    int
	queue      string
	run        runnerFunc
	amqpconfig *minimsg.AmqpConfig
}

// NewWorker instantiates a new worker
// By default:
// - the worker's name is os.Hostname(), fallback is "miniworker"
// - threads are set to 1
func NewWorker(log *tinylog.Tiny) *Worker {
	var hostname string
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "miniworker"
	}
	return &Worker{hostname: hostname, threads: 1, log: log}
}

// SetHostname sets the worker's hostname
func (w *Worker) SetHostname(hostname string) {
	w.hostname = hostname
}

// SetThreads sets the number of worker threads
func (w *Worker) SetThreads(threads int) {
	w.threads = threads
}
func (w *Worker) SetRunnerFunc(runner runnerFunc) {
	w.run = runner
}
func (w *Worker) SetAMQPQueue(Name string) {
	w.queue = Name
}
func (w *Worker) SetAMQPConfig(c *minimsg.AmqpConfig) {
	w.amqpconfig = c
}
func (w *Worker) RunWorker() error {
	w.log.Info(fmt.Sprintf("starting %d instances of consumer", w.threads))
	var err error
	var jobWg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < w.threads; i++ {
		jobWg.Add(1)
		go func(ctx context.Context) {
			var messenger minimsg.MiniMessage
			messenger = &minimsg.AmqpClient{}
			err = messenger.ConnectToBroker(w.amqpconfig)
			if err != nil {
				return
			}
			defer messenger.Close()
			out := make(chan amqp.Delivery)
			messenger.SubscribeToQueue(w.queue, w.hostname, out)
			for {
				select {
				case <-ctx.Done():
					messenger.CloseChannel()
					close(out)
				case d := <-out:
					obj := json.NewDecoder(bytes.NewReader(d.Body))
					var job Job
					if err := obj.Decode(&job); err != nil {
						w.log.NewWarning(fmt.Sprintf("error decoding message: %v\n", err))
						// acknowledge the message as it is malformed
						// and we don't want to infinitely keep attempting to process it
						if err := d.Ack(false); err != nil {
							w.log.NewWarning(fmt.Sprintf("Error acknowledging message : %s", err))
						}
						continue
					}
					w.run(w.log, job)
					// acknowledge the job has finished.
					if err := d.Ack(false); err != nil {
						w.log.NewWarning(fmt.Sprintf("Error acknowledging message : %s", err))
					}
				}
			}
		}(ctx)
		if err != nil {
			cancel()
			return err
		}
		jobWg.Done()
	}
	// catch os exit signal and exit gracefully
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-exit
	cancel()
	return nil
}
