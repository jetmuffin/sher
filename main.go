package main

import (
	"flag"
	"os"

	"github.com/gogo/protobuf/proto"

	. "github.com/JetMuffin/tasting/scheduler"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
)

var (
	master       = flag.String("address", "127.0.0.1", "Master address <ip:port>")
	executorPath = flag.String("executor", "./executor", "Path to test executor")
)

func init() {
	flag.Parse()
}

func main() {

	// Executor
	executor := &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor"),
		Command: &mesos.CommandInfo{
			Value: proto.String(*executorPath),
		},
	}

	// Scheduler
	newTestScheduler()
	scheduler, err := newTestScheduler(executor, CPUS_PER_TASK, MEM_PER_TASK)
	if err != nil {
		log.Fatalf("Failed to create scheduler with error: %v\n", err)
		os.Exit(-2)
	}

	// Framework
	frameworkInfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Mesos Test Framework"),
	}

	// Scheduler Driver
	config := sched.DriverConfig{
		Scheduler: scheduler,
		Framework: frameworkInfo,
		Master:    *master,
	}

	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Fatalf("Unable to create a SchedulerDriver: %v\n", err)
		os.Exit(-3)
	}

	if stat, err := driver.Run(); err != nil {
		log.Fatalf("Framework stopped with status %s and error %s\n", stat.String(), err.Error())
		os.Exit(-4)
	}
}
