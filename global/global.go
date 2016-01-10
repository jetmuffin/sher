// Global variables and configuration of Sher.
package global

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "net"
    "sync"

    "github.com/Unknwon/goconfig"
)

var Master			string 		// Mesos master address
var Address 		string		// Binding address for artifact server
var ExecutorPath	string 		// Path of executor binary file
var WorkDir			string		// Path of workspace direction

var once sync.Once

// Read config file.
func loadConfig(fileName string) error {

    configPath, err := filepath.Abs(fileName)
    fmt.Println(configPath)
    if err != nil {
        return err
    }

    configs, err := goconfig.LoadConfigFile(fileName)
    if err != nil {
        return fmt.Errorf("Cannot open config file: %s\n", fileName)
    }

 	// master
    Master = configs.MustValue("", "master", "127.0.0.1:5050")
    
    // ip
    ip := configs.MustValue("", "ip")
    if ip == "auto" {
        ip, err = getIPAutomaticly()
        if err != nil {
            return err
        }
        if ip == "" {
            return fmt.Errorf("Cannot get IP address.\n")
        }
    } else if ip == "" {
        ip = "127.0.0.1"
    }
    port := configs.MustInt("", "port")
    Address = fmt.Sprintf("%s:%d", ip, port)

    // workdir
    WorkDir = configs.MustValue("", "workdir")

    // executor path
    ExecutorPath = configs.MustValue("", "executorPath")

    return nil
}

// Get first IPv4 address in system's network interface.
// This may be a Lan IP or a public IP.
func getIPAutomaticly() (a string, e error) {
    addr, e := net.InterfaceAddrs()
    if e != nil {
        return
    }
    for _, i := range addr {
        ip := net.ParseIP(strings.SplitN(i.String(), "/", 2)[0])
        ipString := ip.String()
        if ip.To4() != nil && !ip.IsLoopback() && ipString != "0.0.0.0" {
            a = ipString
            goto END
        }
    }

    END:
    if a == "" {
        a = "127.0.0.1"
    }
    return
}

func init() {
    once.Do(func() {
    	err := loadConfig("config.ini")
        if err != nil {
            fmt.Fprintf(os.Stderr, err.Error())
            os.Exit(1)
        }
    })
}