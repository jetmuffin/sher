package test

import (
	"testing"
	"fmt"
	"github.com/JetMuffin/sher/manager"
)

func TestManager(t *testing.T) {
	m := manager.NewManager()
	task := manager.TestTask("1")
	
	// test add task
	fmt.Println("Test case #1")
	err := m.AddTask(task.ID, task)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Println("add task successful")
	}

	// test get non-exist task
	fmt.Println("Test case #2")
	task, err = m.GetTask("2")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("%v\n", task)	
	}

	// test get exists task
	fmt.Println("Test case #3")
	task, err = m.GetTask("1")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("%v\n", task)
	}

	// test update task
	fmt.Println("Test case #4")
	task.Command = "sleep"
	err = m.UpdateTask("1", task)
	task, err = m.GetTask("1")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("%v\n", task)
	}

	fmt.Println("Test case #5")
	err = m.DeleteTask("1")
	task, err = m.GetTask("1")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("%v\n", task)	
	}	
}
