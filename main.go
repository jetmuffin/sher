package main

import (
	"flag"
	"github.com/gogo/protobuf/proto"
	"os"
	"net"

	. "github.com/JetMuffin/test-go-framework/scheduler"
	. "github.com/JetMuffin/test-go-framework/server"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
	defaultArtifactPort = 8000
)

var (
	address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", defaultArtifactPort, "Binding port for artifact server")
	master       = flag.String("master", "127.0.0.1", "Master address <ip:port>")
	executorPath = flag.String("executor", "./executor", "Path to test executor")
	shellPath 	 = flag.String("shell", "./test", "Path to test case shell")
)

func init() {
	flag.Parse()
}

func main() {

	// Start HTTP server hosting executor binary
	executorUri := ServeFileArtifact(*address, *artifactPort, *executorPath)
	shellUri := ServeFileArtifact(*address, *artifactPort, *shellPath)

	// Executor
	exec := prepareExecutorInfo(executorUri, getExecutorCmd(*executorPath))

	// Scheduler
	scheduler, err := NewTestScheduler(exec, shellUri, CPUS_PER_TASK, MEM_PER_TASK)
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

func prepareExecutorInfo(uri string, cmd string) *mesos.ExecutorInfo {
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
			Value: proto.String(cmd),
			Uris:  executorUris,
		},
	}
}

func getExecutorCmd(path string) string {
	return "." + GetHttpPath(path)
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}
