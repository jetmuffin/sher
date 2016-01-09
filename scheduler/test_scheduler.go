// package scheduler

// import (
// 	"github.com/gogo/protobuf/proto"
// 	"strconv"

// 	log "github.com/Sirupsen/logrus"
// 	mesos "github.com/mesos/mesos-go/mesosproto"
// 	util "github.com/mesos/mesos-go/mesosutil"
// 	sched "github.com/mesos/mesos-go/scheduler"
// )

// type TestScheduler struct {
// 	executor      *mesos.ExecutorInfo
// 	tasksLaunched int
// 	tasksFinished int
// 	totalTasks    int
// 	commands      []string
// 	cpuPerTask    float64
// 	memPerTask    float64
// }

// func newTestScheduler(exec *mesos.ExecutorInfo, cpuPerTask float64, memPerTask float64) (*TestScheduler, error) {
// 	commands, err := readLines("commands")
// 	if err != nil {
// 		log.Errorf("Error : %v\n", err)
// 		return nil, err
// 	}

// 	return &TestScheduler{
// 		executor:      exec,
// 		tasksLaunched: 0,
// 		tasksFinished: 0,
// 		totalTasks:    len(commands),
// 		commands:      commands,
// 		cpuPerTask:    cpuPerTask,
// 		memPerTask:    memPerTask,
// 	}, nil
// }

// func (sched *TestScheduler) Registered(_ sched.SchedulerDriver, frameworkID *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
// 	log.Infoln("Scheduler registered with Master ", masterInfo)
// }

// func (sched *TestScheduler) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
// 	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
// }

// func (sched *TestScheduler) Disconnected(sched.SchedulerDriver) {
// 	log.Infoln("Scheduler disconnected with Master")
// }

// func (sched *TestScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
// 	logOffers(offers)

// 	if sched.tasksLaunched >= sched.totalTasks {
// 		return
// 	}

// 	for _, offer := range offers {
// 		remainingCpus := getOfferScalar(offer, "cpus")
// 		remainingMems := getOfferScalar(offer, "mem")

// 		var tasks []*mesos.TaskInfo
// 		for sched.cpuPerTask <= remainingCpus &&
// 			sched.memPerTask <= remainingMems &&
// 			sched.tasksLaunched < sched.totalTasks {

// 			log.Infof("Launch command %v of %v\n", sched.tasksLaunched, sched.totalTasks)

// 			command := sched.commands[sched.tasksLaunched]
// 			sched.tasksLaunched++

// 			taskId := &mesos.TaskID{
// 				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
// 			}

// 			task := &mesos.TaskInfo{
// 				Name:     proto.String("test-task-" + taskId.GetValue()),
// 				TaskId:   taskId,
// 				SlaveId:  offer.SlaveId,
// 				Executor: sched.executor,
// 				Resources: []*mesos.Resource{
// 					util.NewScalarResource("cpus", sched.cpuPerTask),
// 					util.NewScalarResource("mems", sched.memPerTask),
// 				},
// 				Data: []byte(command),
// 			}
// 			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

// 			tasks = append(tasks, task)
// 			remainingCpus -= sched.cpuPerTask
// 			remainingMems -= sched.memPerTask
// 		}
// 		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
// 		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
// 	}
// }

// func (sched *TestScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
// 	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

// 	if status.GetState() == mesos.TaskState_TASK_FINISHED {
// 		sched.tasksFinished++
// 		log.Infof("%v of %v tasks finished.", sched.tasksFinished, sched.totalTasks)
// 	}

// 	if sched.tasksFinished >= sched.totalTasks {
// 		log.Infoln("Total tasks completed, stopping framework.")
// 		driver.Stop(false)
// 	}

// 	if status.GetState() == mesos.TaskState_TASK_LOST ||
// 		status.GetState() == mesos.TaskState_TASK_KILLED ||
// 		status.GetState() == mesos.TaskState_TASK_FAILED {
// 		log.Infoln(
// 			"Aborting because task", status.TaskId.GetValue(),
// 			"is in unexpected state", status.State.String(),
// 			"with message", status.GetMessage(),
// 		)
// 		driver.Abort()
// 	}
// }

// func (sched *TestScheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
// 	log.Printf("Offer rescinded: %s", offerID)
// }

// func (sched *TestScheduler) FrameworkMessage(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, message string) {
// 	log.Printf("Received framework message from %s %s: %s", executorID, slaveID, message)
// }

// func (sched *TestScheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
// 	log.Printf("Slave lost: %s", slaveID)
// }

// func (sched *TestScheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, _ int) {
// 	log.Printf("Executor lost: %s %s", executorID, slaveID)
// }

// func (sched *TestScheduler) Error(driver sched.SchedulerDriver, err string) {
// 	log.Printf("Error: %s", err)
// }

package scheduler

import (
	"github.com/gogo/protobuf/proto"
	"strconv"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

type TestScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	commands      []string
	cpuPerTask    float64
	memPerTask    float64
}

func NewTestScheduler(exec *mesos.ExecutorInfo, cpuPerTask float64, memPerTask float64) (*TestScheduler, error) {
	commands, err := readLines("commands")
	if err != nil {
		log.Errorf("Error : %v\n", err)
		return nil, err
	}

	return &TestScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    len(commands),
		commands:      commands,
		cpuPerTask:    cpuPerTask,
		memPerTask:    memPerTask,
	}, nil
}

func (sched *TestScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
}

func (sched *TestScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *TestScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (sched *TestScheduler) processOffer(driver sched.SchedulerDriver, offer *mesos.Offer) {
	remainingCpus := getOfferScalar(offer, "cpus")
	remainingMems := getOfferScalar(offer, "mem")

	if sched.tasksLaunched >= sched.totalTasks ||
		remainingCpus < sched.cpuPerTask ||
		remainingMems < sched.memPerTask {
		driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}

	// At this point we have determined we will be accepting at least part of this offer
	var tasks []*mesos.TaskInfo

	for sched.cpuPerTask <= remainingCpus &&
		sched.memPerTask <= remainingMems &&
		sched.tasksLaunched < sched.totalTasks {

		log.Infof("Processing image %v of %v\n", sched.tasksLaunched, sched.totalTasks)
		command := sched.commands[sched.tasksLaunched]
		sched.tasksLaunched++

		taskId := &mesos.TaskID{
			Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
		}

		task := &mesos.TaskInfo{
			Name:     proto.String("go-task-" + taskId.GetValue()),
			TaskId:   taskId,
			SlaveId:  offer.SlaveId,
			Executor: sched.executor,
			Resources: []*mesos.Resource{
				util.NewScalarResource("cpus", sched.cpuPerTask),
				util.NewScalarResource("mem", sched.memPerTask),
			},
			Data: []byte(command),
		}
		log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

		tasks = append(tasks, task)
		remainingCpus -= sched.cpuPerTask
		remainingMems -= sched.memPerTask
	}

	log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
	driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
}

func (sched *TestScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		log.Infof("Received Offer <%v> with cpus=%v mem=%v", offer.Id.GetValue(), getOfferScalar(offer, "cpus"), getOfferScalar(offer, "mem"))
		sched.processOffer(driver, offer)
	}
}

func (sched *TestScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
		log.Infof("%v of %v tasks finished.", sched.tasksFinished, sched.totalTasks)
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		driver.Abort()
	}
}

func (sched *TestScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *TestScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *TestScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (sched *TestScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (sched *TestScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}
