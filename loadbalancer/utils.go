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
		log.Fatalln("Failed to build server image: ", err)
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
		log.Fatalf("Failed to start new server instance: %v, stderr: %s", err, stderr.String())
	}
}

func getServerIP(hostname string) string {
	cmd := exec.Command("sudo", "docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", hostname)
	output, err := cmd.Output()
	if err != nil {
		log.Fatalln("Error running docker inspect: ", err)
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
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(ServerPort)+"/config", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Println("Error configuring Server:", err)
		return
	}
	defer resp.Body.Close()
}

func removeServerInstance(hostname string) {
	cmd := exec.Command("sudo", "docker", "stop", hostname)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to stop server instance '%s': %v\n", hostname, err)
		return
	}

	cmd = exec.Command("sudo", "docker", "rm", hostname)
	err = cmd.Run()
	if err != nil {
		log.Fatalf("Failed to remove server instance '%s': %v\n", hostname, err)
		return
	}
}

func cleanupServers(serverIDs []int) {
	log.Println("Cleaning up server instances...")

	for _, server := range serverIDs {
		stopCmd := exec.Command("sudo", "docker", "stop", fmt.Sprintf("Server%d", server))
		removeCmd := exec.Command("sudo", "docker", "rm", fmt.Sprintf("Server%d", server))

		if err := stopCmd.Run(); err != nil {
			log.Printf("Failed to stop server '%d': %v\n", server, err)
		}
		if err := removeCmd.Run(); err != nil {
			log.Printf("Failed to remove server '%d': %v\n", server, err)
		}
	}
}
