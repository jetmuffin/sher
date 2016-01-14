package manager

import (
	"github.com/JetMuffin/sher/mesosproto"
)
type Task struct {
	ID          	string   		`json:"id"`
	DockerImage 	string   		`json:"docker_image"`
	Command     	string   		`json:"cmd"`
	Cpus        	float64  		`json:"cpus,string"`
	Disk        	float64  		`json:"disk,string"`
	Mem         	float64  		`json:"mem,string"`
	Volumes			Volume 			`json:"volumes,omitempty"`
	Arguments     	[]string  		`json:"arguments,omitempty"`
	Ports			Port 			`json:"ports,omitempty"`

	DockerID		string
	DockerName		string
	SlaveID     	string
	CreatedTime     int64
	TaskInfo		*mesosproto.TaskInfo
	Running			bool
}

func TestTask(id string) *Task {
	return &Task{
		ID: 			id,
		DockerImage: 	"ubuntu",
		Command:		"echo `hello sher`",
		Cpus:			0.5,
		Disk:			0,
		Mem:			16,
	}
}
