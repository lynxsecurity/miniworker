package miniworker

import (
	"fmt"
	"os"
	"testing"

	"github.com/lynxsecurity/minimsg"
	"github.com/lynxsecurity/tinylog"
)

func Run(job Job) error {
	fmt.Println("Job domain: ", job.Domain)
	return nil
}
func TestWorkerMain(t *testing.T) {
	log := tinylog.New(os.Stdout)
	w := NewWorker(log)
	w.SetAMQPConfig(&minimsg.AmqpConfig{Host: "", Port: 5672, Username: "", Password: ""})
	w.SetThreads(5)
	w.SetAMQPQueue("queueName")
	w.SetRunnerFunc(Run)
	err := w.RunWorker()
	if err != nil {
		t.Error(err)
	}
}
