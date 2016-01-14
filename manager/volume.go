package manager

type Volume struct {
	ContainerPath string `json:"container_path"`
	HostPath      string `json:"host_path"`
	Mode          string `json:"mode"` //rw or ro
}
