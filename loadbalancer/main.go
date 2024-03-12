package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"

	"github.com/Sarita-Singh/galaxyDB/loadbalancer/internal/consistenthashmap"
)

const ServerDockerImageName = "galaxydb-server"
const DockerNetworkName = "galaxydb-network"
const ServerPort = 5000
const N = 3

var chm = consistenthashmap.NewConsistentHashMap() // Initialize your consistent hash map

// ServerConfig and ShardConfig structures represent the configuration
// of servers and shards as provided in the init request.
type ServerConfig struct {
	ID       int      `json:"id"`
	Hostname string   `json:"hostname"`
	Shards   []string `json:"shards"`
}

type ShardConfig struct {
	StudIDLow int    `json:"Stud_id_low"`
	ShardID   string `json:"Shard_id"`
	ShardSize int    `json:"Shard_size"`
}

type SchemaConfig struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

type InitRequest struct {
	N       int                 `json:"N"`
	Schema  SchemaConfig        `json:"schema"`
	Shards  []ShardConfig       `json:"shards"`
	Servers map[string][]string `json:"servers"`
}

var (
	serverConfigs []ServerConfig
	shardConfigs  []ShardConfig
	serverMutex   sync.Mutex

	configData struct {
		Servers []ServerConfig `json:"servers"`
		Shards  []ShardConfig  `json:"shards"`
	}
	configMutex sync.RWMutex
)

func initHandler(w http.ResponseWriter, r *http.Request) {
	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	serverMutex.Lock()
	configMutex.Lock() // Ensure thread safety
	defer serverMutex.Unlock()
	defer configMutex.Unlock()

	// Convert map to slice for internal and configData storage
	convertedServers := []ServerConfig{}
	for serverName, shardIDs := range req.Servers {
		convertedServer := ServerConfig{
			Hostname: serverName,
			ID:       getNextServerID(),
			Shards:   shardIDs,
		}
		convertedServers = append(convertedServers, convertedServer)
	}

	// Now you have a slice of ServerConfig from the map
	serverConfigs = convertedServers
	shardConfigs = req.Shards

	// Update configData with converted slice and shards directly
	configData.Servers = convertedServers
	configData.Shards = req.Shards

	// Initialize shards in the consistent hash map
	for _, shard := range req.Shards {
		var serversForShard []string
		for _, server := range convertedServers {
			for _, serverShard := range server.Shards {
				if serverShard == shard.ShardID {
					serversForShard = append(serversForShard, strconv.Itoa(server.ID))
					break
				}
			}
		}
		chm.AddShard(shard.ShardID, serversForShard)
	}

	// Respond with success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Database initialized successfully", "status": "success"})
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	configMutex.RLock() // Use read lock for concurrency safety
	defer configMutex.RUnlock()

	// Serve the stored configuration data
	response := map[string]interface{}{
		"N": len(configData.Servers), // Assuming "N" represents the number of servers
		"schema": map[string][]string{
			"columns": []string{"Stud_id", "Stud_name", "Stud_marks"},
			"dtypes":  []string{"Number", "String", "String"},
		},
		"shards":  configData.Shards,
		"servers": configData.Servers,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type AddRequest struct {
	N         int                 `json:"n"`
	NewShards []ShardConfig       `json:"new_shards"`
	Servers   map[string][]string `json:"servers"` // ServerID to list of ShardIDs
}

type AddResponse struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type AddMapResponse struct {
	Message map[string]interface{} `json:"message"`
	Status  string                 `json:"status"`
}

func spawnNewServerInstance(hostname string, id int) {
	cmd := exec.Command("sudo", "docker", "build", "--tag", ServerDockerImageName, "/server")
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to build server image: %v", err)
	}

	// Run the server Docker container
	cmd = exec.Command("sudo", "docker", "run", "-d", "--name", hostname, "--network", DockerNetworkName, "-e", fmt.Sprintf("id=%d", id), fmt.Sprintf("%s:latest", ServerDockerImageName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		// log.Fatalf("Failed to start new server instance: %v", err)
		log.Printf("Failed to start new server instance: %v, stderr: %s", err, stderr.String())
	}
}

func getNextServerID() int {
	return rand.Intn(900000) + 100000
}

func addServersEndpoint(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	configMutex.Lock()
	defer configMutex.Unlock()

	// Sanity check: ensure the number of servers to add does not exceed the request count
	if len(req.Servers) < req.N {
		resp := AddMapResponse{
			Message: map[string]interface{}{"<Error>": "Number of new servers (n) is greater than newly added instances"},
			Status:  "failure",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Handle adding new shards
	for _, shard := range req.NewShards {
		shardConfigs = append(shardConfigs, shard)
		var serversForShard []string
		for serverName, shardIDs := range req.Servers {
			serverID := -1
			for _, serverConfig := range configData.Servers {
				if serverConfig.Hostname == serverName {
					serverID = serverConfig.ID
					break
				}
			}
			for _, shardID := range shardIDs {
				if shardID == shard.ShardID {
					serversForShard = append(serversForShard, strconv.Itoa(serverID))
					break
				}
			}
		}
		chm.AddShard(shard.ShardID, serversForShard)
	}

	// Handle adding new servers
	serverIDsAdded := []int{}
	for serverName, shardIDs := range req.Servers {
		// Assume spawnNewServerInstance creates or updates server instances as needed
		serverID := getNextServerID()
		// Check if the serverName conforms to the allowed characters
		matched, err := regexp.MatchString(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`, serverName)
		if err != nil {
			log.Fatalf("Regex match error: %v", err)
		}

		if !matched {
			// If serverName doesn't match, assign a new compliant hostname
			serverName = fmt.Sprintf("Server-%d", serverID)
		}
		spawnNewServerInstance(serverName, serverID)

		// chm.AddServer(strconv.Itoa(serverID), shardIDs)

		serverConfigs = append(serverConfigs, ServerConfig{ID: serverID, Hostname: serverName, Shards: shardIDs})
		serverIDsAdded = append(serverIDsAdded, serverID)
	}

	message := fmt.Sprintf("Added Servers: %v", serverIDsAdded)
	json.NewEncoder(w).Encode(AddResponse{
		Message: message,
		Status:  "successful",
	})
	w.WriteHeader(http.StatusOK)
}

type RemoveRequest struct {
	N       int      `json:"n"`
	Servers []string `json:"servers"` // List of server names to remove
}

type RemoveResponse struct {
	Message map[string]interface{} `json:"message"`
	Status  string                 `json:"status"`
}

// Removes a server instance by name. Returns true if removal was successful.
func removeServerInstance(hostname string) bool {
	cmd := exec.Command("sudo", "docker", "stop", hostname)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to stop server instance '%s': %v", hostname, err)
		return false
	}

	cmd = exec.Command("sudo", "docker", "rm", hostname)
	err = cmd.Run()
	if err != nil {
		log.Fatalf("Failed to remove server instance '%s': %v", hostname, err)
		return false
	}

	serverID := -1
	for _, serverConfig := range configData.Servers {
		if serverConfig.Hostname == hostname {
			serverID = serverConfig.ID
			break
		}
	}

	// Update the consistent hash map to remove the server's virtual nodes and shard associations
	// chm.RemoveServer(strconv.Itoa(serverID))

	// Lock global config data for thread-safe operation
	configMutex.Lock()
	defer configMutex.Unlock()

	// Update serverConfigs to reflect removal
	for i, serverConfig := range configData.Servers {
		if serverConfig.ID == serverID {
			configData.Servers = append(configData.Servers[:i], configData.Servers[i+1:]...)
			break // Assuming server IDs are unique, can break after finding
		}
	}

	return true
}

// Randomly chooses a server to remove
func chooseRandomServer() string {
	if len(configData.Servers) == 0 {
		return ""
	}
	index := rand.Intn(len(configData.Servers))
	return configData.Servers[index].Hostname
}

func removeServersEndpoint(w http.ResponseWriter, r *http.Request) {
	var req RemoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	configMutex.Lock()
	defer configMutex.Unlock()

	// Sanity check: If the provided server list is longer than the number of servers to remove
	if len(req.Servers) > req.N {
		json.NewEncoder(w).Encode(RemoveResponse{
			Message: map[string]interface{}{"<Error>": "Length of server list is more than removable instances"},
			Status:  "failure",
		})
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Track removed servers for response
	removedServers := make([]string, 0, len(req.Servers))

	// Remove specified servers
	for _, serverName := range req.Servers {
		if removeServerInstance(serverName) {
			removedServers = append(removedServers, serverName)
		}
	}

	// If additional servers need to be randomly removed
	additionalRemovalsNeeded := req.N - len(removedServers)
	for additionalRemovalsNeeded > 0 {
		// Choose a server randomly and remove it
		if serverName := chooseRandomServer(); serverName != "" {
			if removeServerInstance(serverName) {
				removedServers = append(removedServers, serverName)
				additionalRemovalsNeeded--
			}
		}
	}

	response := RemoveResponse{
		Message: map[string]interface{}{
			"N":       len(configData.Servers) - len(removedServers),
			"servers": removedServers,
		},
		Status: "successful",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to send response: %v", err)
		// Handle error in sending response if necessary.
	}
}

// cleanupServers stops and removes all server containers
func cleanupServers() {
	fmt.Println("Cleaning up server instances...")
	for _, server := range configData.Servers {
		stopCmd := exec.Command("sudo", "docker", "stop", server.Hostname)
		removeCmd := exec.Command("sudo", "docker", "rm", server.Hostname)

		if err := stopCmd.Run(); err != nil {
			fmt.Printf("Failed to stop server '%s': %v", server.Hostname, err)
		}
		if err := removeCmd.Run(); err != nil {
			fmt.Printf("Failed to remove server '%s': %v", server.Hostname, err)
		}
	}
}

func main() {
	// a channel to listen to OS signal - ctrl+C to exit
	sigs := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs // termination signal
		cleanupServers()
		cleanupDone <- true // Signal that cleanup is done
	}()

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/add", addServersEndpoint)
	http.HandleFunc("/rm", removeServersEndpoint)

	fmt.Println("Load Balancer running on port 5000")
	log.Fatal(http.ListenAndServe(":5000", nil))

	// Waiting for cleanup to be done before exiting
	<-cleanupDone
}
