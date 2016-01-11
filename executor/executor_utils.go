package main

import (
	"fmt"
	"os/exec"
    "strings"
    "net/http"
    "io"
    "os"
	"io/ioutil"
)

func downloadFile(url string) (string, error) {
    // TODO fix bug
    downloadUrl := fmt.Sprintf("http://%s/%s", "192.168.33.10:8000", url)
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

func readFile(fileName string) {
    f, err := os.Open("/tmp/" + fileName)
    if err != nil {
        panic("open failed!")
    }
    defer f.Close()
    buff := make([]byte, 1024)
    for n, err := f.Read(buff); err == nil; n, err = f.Read(buff) {
        fmt.Print(string(buff[:n]))
    }
    if err != nil {
        panic(fmt.Sprintf("Read occurs error: %s", err))
    }
}

func writeFile(fileName string, content string) {
    f, err := os.Create(fileName)
    defer f.Close()
    if err != nil {
        fmt.Println(fileName, err)
        return
    }
    f.WriteString(content)
}

func runCommand(fileName string) (string, error) {
	cmd := exec.Command("/bin/bash", "/tmp/" + fileName)
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