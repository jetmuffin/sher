package task

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
)
type Task struct {
	ID          string   		`json:"id"`
	DockerImage string   		`json:"docker_image"`
	Command     string   		`json:"cmd"`
	Cpus        float64  		`json:"cpus,string"`
	Disk        float64  		`json:"disk,string"`
	Mem         float64  		`json:"mem,string"`
	Volumes		*mesos.Volume 	`json:"volumes,omitempty"`
	Ports		*mesos.Ports 	`json:"ports,omitempty"`

	DockerID	string
	DockerName	string
	SlaveID     string
	TaskInfo	*mesos.TaskInfo
	Running		bool
}