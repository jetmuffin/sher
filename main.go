package main

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"flag"
	"strings"
	"os"

	. "github.com/JetMuffin/sher/scheduler"
	. "github.com/JetMuffin/sher/server"
	log "github.com/golang/glog"
	global "github.com/JetMuffin/sher/global"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
	defaultArtifactPort = 8000
)

func init() {
	flag.Parse()
}

func main() {

	// Start HTTP server
	fmt.Printf("HTTP Server run on %v\n", global.Address)
	server := NewHttpServer(global.Address, global.WorkDir)
	server.Listen()

	// Executor
	executorUri := fmt.Sprintf("http://%s/%s", global.Address, global.ExecutorPath)
	exec := prepareExecutorInfo(executorUri)

	// Scheduler
	scheduler, err := NewMesosScheduler(exec, CPUS_PER_TASK, MEM_PER_TASK)
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
		Master:    global.Master,
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

func prepareExecutorInfo(uri string) *mesos.ExecutorInfo {
	executorUris := []*mesos.CommandInfo_URI{
		{
			Value:      &uri,
			Executable: proto.Bool(true),
		},
	}

	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String(getExecutorCmd(global.ExecutorPath)),
			Uris:  executorUris,
		},
	}
}

func getExecutorCmd(path string) string {
	// spilit base path from executorPath
	// Example: ./exec
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}

	return "./" + base
}

