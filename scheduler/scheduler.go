package scheduler

import (
	"github.com/gogo/protobuf/proto"
	"strconv"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

type MesosScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	commands      []string
	cpuPerTask    float64
	memPerTask    float64
}

func NewMesosScheduler(exec *mesos.ExecutorInfo, cpuPerTask float64, memPerTask float64) (*MesosScheduler, error) {
	commands, err := readLines("commands")
	if err != nil {
		log.Errorf("Error : %v\n", err)
		return nil, err
	}

	return &MesosScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    len(commands),
		commands:      commands,
		cpuPerTask:    cpuPerTask,
		memPerTask:    memPerTask,
	}, nil
}

func (sched *MesosScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
}

func (sched *MesosScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *MesosScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (sched *MesosScheduler) processOffer(driver sched.SchedulerDriver, offer *mesos.Offer) {
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

		log.Infof("Processing command %v of %v\n", sched.tasksLaunched+1, sched.totalTasks)
		commandFile := sched.commands[sched.tasksLaunched]
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
			// TODO
			Data: []byte(commandFile),
		}
		log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

		tasks = append(tasks, task)
		remainingCpus -= sched.cpuPerTask
		remainingMems -= sched.memPerTask
	}

	log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
	driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
}

func (sched *MesosScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		log.Infof("Received Offer <%v> with cpus=%v mem=%v", offer.Id.GetValue(), getOfferScalar(offer, "cpus"), getOfferScalar(offer, "mem"))
		sched.processOffer(driver, offer)
	}
}

func (sched *MesosScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	// TODO 
	// extract function of taskManager
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

func (sched *MesosScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *MesosScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *MesosScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (sched *MesosScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (sched *MesosScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}
