package main

import (
	"fmt"
	"os/exec"
    "strings"
    "path/filepath"
    "net/http"
    "io"
    "os"
	"io/ioutil"

    global "github.com/JetMuffin/sher/global"
)

func downloadFile(url string) (string, error) {
    downloadUrl := fmt.Sprintf("http://%s/%s", global.Address, url)
    tokens := strings.Split(url, "/")
    fileName := tokens[len(tokens)-1]
    fmt.Println("Downloading", downloadUrl, "to", fileName)

    output, err := os.Create("/tmp/" + fileName)
    if err != nil {
        fmt.Println("Error while creating", fileName, "-", err)
        return "", err
    }
    defer output.Close()

    response, err := http.Get(downloadUrl)
    if err != nil {
        fmt.Println("Error while downloading", url, "-", err)
        return "", err
    }
    defer response.Body.Close()

    n, err := io.Copy(output, response.Body)
    if err != nil {
        fmt.Println("Error while downloading", url, "-", err)
        return "", err
    }

    fmt.Println(n, "bytes downloaded.")

    return fileName, nil
}

func runCommand(url string) (string, error) {
	cmd := exec.Command("/bin/bash", url)
	stdout, err := cmd.StdoutPipe()
    if err != nil {
        return "", err
    }
 
    stderr, err := cmd.StderrPipe()
    if err != nil {
        return "", err
    }
 
    if err := cmd.Start(); err != nil {
        return "", err
    }
 
    bytesErr, err := ioutil.ReadAll(stderr)
    if err != nil {
        return "", err
    }
 
    if len(bytesErr) != 0 {
        return "", err
    }
 
    bytes, err := ioutil.ReadAll(stdout)
    if err != nil {
        return "", err
    }
 
    if err := cmd.Wait(); err != nil {
        return "", err
    }
 
    return string(bytes), nil
}