package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
)

type configPayload struct {
	Schema SchemaConfig `json:"schema"`
	Shards []string     `json:"shards"`
}

func buildServerInstance() {
	cmd := exec.Command("sudo", "docker", "build", "--tag", ServerDockerImageName, "/server")
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to build server image: %v", err)
	} else {
		log.Println("Server image built successfully")
	}
}

func spawnNewServerInstance(hostname string, id int) {
	cmd := exec.Command("sudo", "docker", "run", "-d", "--name", hostname, "--network", DockerNetworkName, "-e", fmt.Sprintf("id=%d", id), fmt.Sprintf("%s:latest", ServerDockerImageName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Printf("Failed to start new server instance: %v, stderr: %s", err, stderr.String())
	}
}

func getServerIP(hostname string) string {
	cmd := exec.Command("sudo", "docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", hostname)
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("Error running docker inspect: %v\n", err)
		return ""
	}

	return strings.TrimSpace(string(output))
}

func configNewServerInstance(serverID int, shards []string, schema SchemaConfig) {
	payload := configPayload{
		Schema: schema,
		Shards: shards,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("Error marshaling JSON: ", err)
		return
	}

	resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(ServerPort)+"/config", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		fmt.Println("Error configuring Server:", err)
		return
	}
	defer resp.Body.Close()
}
