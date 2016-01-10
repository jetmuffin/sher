package main

import (
	"flag"
	"fmt"

	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type TestExecutor struct {
	tasksLaunched int
}

func newTestExecutor() *TestExecutor {
	return &TestExecutor{
		tasksLaunched: 0,
	}
}

func (exec *TestExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *TestExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *TestExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *TestExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Printf("Launching task %v with data [%#x]\n", taskInfo.GetName(), taskInfo.Data)

	// Start task
	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error :", err)
	}

	exec.tasksLaunched++
	fmt.Println("Total tasks launched ", exec.tasksLaunched)

	// Download command shell file
	fileName, err := downloadFile(string(taskInfo.Data))
	if err != nil {
		fmt.Printf("Failed to download test script with error : %v\n", err)
		return
	}
	fmt.Printf("Downloaded test shell file: %v\n", fileName)

	// run command
	stdout, err := runCommand(fileName)
	if err != nil {
		fmt.Println("Command error :", err.Error())
	}
	fmt.Printf("Test result: %v\n", stdout)

	// finish task
	fmt.Println("Finishing task", taskInfo.GetName())
	finStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_FINISHED.Enum(),
	}
	_, err = driver.SendStatusUpdate(finStatus)
	if err != nil {
		fmt.Println("Got error :", err)
		return
	}

	fmt.Println("Task finished", taskInfo.GetName())
}

func (exec *TestExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *TestExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (exec *TestExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *TestExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func init() {
	flag.Parse()
}

func main() {
	fmt.Println("Starting Test Executor")

	driverConfig := exec.DriverConfig{
		Executor: newTestExecutor(),
	}
	driver, err := exec.NewMesosExecutorDriver(driverConfig)

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error: ", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}
