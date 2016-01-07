package main

import (
	"flag"
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"

	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

const (
	MemPerTask = 128
	CPUPerTask = 1
)

type testScheduer struct {
	tasksLaunched  int
	currentTaskIDs []*mesos.TaskID
}

func newTestScheduler() *testScheduer {
	return &testScheduer{
		tasksLaunched:  0,
		currentTaskIDs: []*mesos.TaskID{},
	}
}

func (s *testScheduer) Registered(_ sched.SchedulerDriver, frameworkID *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework registered with Master ", masterInfo)
}

func (s *testScheduer) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}

func (s *testScheduer) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Framework disconnected with Master")
}

func (s *testScheduer) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		var tasks []*mesos.TaskInfo
		if mems >= MemPerTask && cpus >= CPUPerTask {
			var taskID *mesos.TaskID
			var task *mesos.TaskInfo

			//launch a task
			s.tasksLaunched++
			taskID = &mesos.TaskID{
				Value: proto.String("test-" + strconv.Itoa(s.tasksLaunched)),
			}
			task = &mesos.TaskInfo{
				Name:    proto.String("task-" + taskID.GetValue()),
				TaskId:  taskID,
				SlaveId: offer.SlaveId,
				Command: &mesos.CommandInfo{
					Shell: proto.Bool(false),
				},
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUPerTask),
					util.NewScalarResource("mem", MemPerTask),
				},
			}
			log.Infof("Prepared task %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			cpus -= CPUPerTask
			mems -= MemPerTask

			tasks = append(tasks, task)
		}
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (s *testScheduer) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (s *testScheduer) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer rescinded: %s", offerID)
}

func (s *testScheduer) FrameworkMessage(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, message string) {
	log.Printf("Received framework message from %s %s: %s", executorID, slaveID, message)
}

func (s *testScheduer) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave lost: %s", slaveID)
}

func (s *testScheduer) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, _ int) {
	log.Printf("Executor lost: %s %s", executorID, slaveID)
}

func (s *testScheduer) Error(driver sched.SchedulerDriver, err string) {
	log.Printf("Error: %s", err)
}

func printUsage() {
	fmt.Println(`
Usage: scheduler -master=Master address<ip:port>
To see a detailed description of the flags available, type "scheduler --help"
	`)
}

func main() {
	master := flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")

	flag.Parse()
	if master == nil {
		printUsage()
		return
	}

	frameworkInfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("A Test Framework"),
	}

	config := sched.DriverConfig{
		Scheduler: newTestScheduler(),
		Framework: frameworkInfo,
		Master:    *master,
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())

		if stat, err := driver.Run(); err != nil {
			log.Infof("Framework stopped with status %s and error : %s", stat.String(), err.Error())
		}
	}
}
