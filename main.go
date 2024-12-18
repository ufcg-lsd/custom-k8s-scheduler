package main

import (
	"os"
	scheduler "qos-driven-scheduler/pkg/qos_driven_scheduler"

	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(scheduler.Name, scheduler.New()),
	)

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
