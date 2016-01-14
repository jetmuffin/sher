package manager

import (
	"errors"
	"sync"
)

var (
	TaskNotExistsErr = errors.New("Specific task not exist")
)

type Manager struct {
	sync.RWMutex
	tasks 	map[string]*Task
}

func NewManager() *Manager {
	return &Manager{
		tasks: make(map[string]*Task),
	}
}

func (manager *Manager) AddTask(id string, task *Task) error {
	manager.Lock()
	defer manager.Unlock()

	manager.tasks[id] = task
	return nil
}

func (manager *Manager) GetTask(id string) (*Task, error) {
	manager.RLock()
	defer manager.RUnlock()

	task, exists := manager.tasks[id]
	if !exists {
		return nil, TaskNotExistsErr
	}

	return task, nil
}

func (manager *Manager) GetAllTasks() ([]*Task, error) {
	manager.RLock()
	defer manager.RUnlock()

	i := 1
	result := make([]*Task, len(manager.tasks))

	for _, task := range manager.tasks {
		result[i] = task
		i++
	}

	return result, nil
}

func (manager *Manager) DeleteTask(id string) error {
	manager.Lock()
	defer manager.Unlock()

	delete(manager.tasks, id)
	return nil
}

func (manager *Manager) UpdateTask(id string, task *Task) error{
	manager.Lock()
	defer manager.Unlock()

	_, exists := manager.tasks[id]
	if !exists {
		return TaskNotExistsErr
	}

	manager.tasks[id] = task	
	return nil
}

