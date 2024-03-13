package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/Sarita-Singh/galaxyDB/loadbalancer/internal/consistenthashmap"
)

const ServerDockerImageName = "galaxydb-server"
const DockerNetworkName = "galaxydb-network"
const ServerPort = 5000
const N = 3

type ShardTConfig struct {
	StudIDLow int    `json:"Stud_id_low"`
	ShardID   string `json:"Shard_id"`
	ShardSize int    `json:"Shard_size"`
	chm       *consistenthashmap.ConsistentHashMap
	mutex     *sync.Mutex
}

type MapTConfig struct {
	ShardID  string `json:"Shard_id"`
	ServerID int    `json:"Server_id"`
}

type SchemaConfig struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

type InitRequest struct {
	N       int                 `json:"N"`
	Schema  SchemaConfig        `json:"schema"`
	Shards  []ShardTConfig      `json:"shards"`
	Servers map[string][]string `json:"servers"`
}

var (
	schemaConfig  SchemaConfig
	shardTConfigs []ShardTConfig
	mapTConfigs   []MapTConfig
)

func getNextServerID() int {
	return rand.Intn(900000) + 100000
}

func getServerID(rawServerName string) int {
	rawServerID := rawServerName[len("Server"):]
	serverID, err := strconv.Atoi(rawServerID)
	if err != nil {
		return getNextServerID()
	}
	return serverID
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	schemaConfig = req.Schema

	for rawServerName, shardIDs := range req.Servers {
		serverID := getServerID(rawServerName)
		for _, shardID := range shardIDs {
			mapTConfigs = append(mapTConfigs, MapTConfig{ShardID: shardID, ServerID: serverID})
		}

		spawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		configNewServerInstance(serverID, shardIDs, req.Schema)
	}

	for _, shard := range req.Shards {
		newShardTConfig := shard

		newShardTConfig.chm = &consistenthashmap.ConsistentHashMap{}
		newShardTConfig.chm.Init()
		for _, mapTConfig := range mapTConfigs {
			if mapTConfig.ShardID == shard.ShardID {
				newShardTConfig.chm.AddServer(mapTConfig.ServerID)
			}
		}
		newShardTConfig.mutex = &sync.Mutex{}

		shardTConfigs = append(shardTConfigs, newShardTConfig)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Configured Database", "status": "success"})
}

func statusHandler(w http.ResponseWriter, _ *http.Request) {

	servers := make(map[string][]string)

	for _, mapTConfig := range mapTConfigs {
		serverName := fmt.Sprintf("Server%d", mapTConfig.ServerID)
		_, contains := servers[serverName]
		if !contains {
			servers[serverName] = []string{mapTConfig.ShardID}
		} else {
			servers[serverName] = append(servers[serverName], mapTConfig.ShardID)
		}
	}

	response := map[string]interface{}{
		"N":       len(servers),
		"schema":  schemaConfig,
		"shards":  shardTConfigs,
		"servers": servers,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type AddRequest struct {
	N         int                 `json:"n"`
	NewShards []ShardTConfig      `json:"new_shards"`
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

// func addServersEndpoint(w http.ResponseWriter, r *http.Request) {
// 	var req AddRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
// 		return
// 	}

// 	// Sanity check: ensure the number of servers to add does not exceed the request count
// 	if len(req.Servers) < req.N {
// 		resp := AddMapResponse{
// 			Message: map[string]interface{}{"<Error>": "Number of new servers (n) is greater than newly added instances"},
// 			Status:  "failure",
// 		}
// 		w.Header().Set("Content-Type", "application/json")
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(resp)
// 		return
// 	}

// 	// Handle adding new shards
// 	for _, shard := range req.NewShards {
// 		shardTConfigs = append(shardTConfigs, shard)
// 		var serversForShard []string
// 		for serverName, shardIDs := range req.Servers {
// 			serverID := -1
// 			for _, serverConfig := range configData.Servers {
// 				if serverConfig.Hostname == serverName {
// 					serverID = serverConfig.ID
// 					break
// 				}
// 			}
// 			for _, shardID := range shardIDs {
// 				if shardID == shard.ShardID {
// 					serversForShard = append(serversForShard, strconv.Itoa(serverID))
// 					break
// 				}
// 			}
// 		}
// 	}

// 	// Handle adding new servers
// 	serverIDsAdded := []int{}
// 	for serverName, shardIDs := range req.Servers {
// 		// Assume spawnNewServerInstance creates or updates server instances as needed
// 		serverID := getNextServerID()
// 		// Check if the serverName conforms to the allowed characters
// 		matched, err := regexp.MatchString(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`, serverName)
// 		if err != nil {
// 			log.Fatalf("Regex match error: %v", err)
// 		}

// 		if !matched {
// 			// If serverName doesn't match, assign a new compliant hostname
// 			serverName = fmt.Sprintf("Server-%d", serverID)
// 		}
// 		spawnNewServerInstance(serverName, serverID)

// 		// chm.AddServer(strconv.Itoa(serverID), shardIDs)

// 		serverConfigs = append(serverConfigs, ServerConfig{ID: serverID, Hostname: serverName, Shards: shardIDs})
// 		serverIDsAdded = append(serverIDsAdded, serverID)
// 	}

// 	message := fmt.Sprintf("Added Servers: %v", serverIDsAdded)
// 	json.NewEncoder(w).Encode(AddResponse{
// 		Message: message,
// 		Status:  "successful",
// 	})
// 	w.WriteHeader(http.StatusOK)
// }

type RemoveRequest struct {
	N       int      `json:"n"`
	Servers []string `json:"servers"` // List of server names to remove
}

type RemoveResponse struct {
	Message map[string]interface{} `json:"message"`
	Status  string                 `json:"status"`
}

// Removes a server instance by name. Returns true if removal was successful.
// func removeServerInstance(hostname string) bool {
// 	cmd := exec.Command("sudo", "docker", "stop", hostname)
// 	err := cmd.Run()
// 	if err != nil {
// 		log.Fatalf("Failed to stop server instance '%s': %v", hostname, err)
// 		return false
// 	}

// 	cmd = exec.Command("sudo", "docker", "rm", hostname)
// 	err = cmd.Run()
// 	if err != nil {
// 		log.Fatalf("Failed to remove server instance '%s': %v", hostname, err)
// 		return false
// 	}

// 	serverID := -1
// 	for _, serverConfig := range configData.Servers {
// 		if serverConfig.Hostname == hostname {
// 			serverID = serverConfig.ID
// 			break
// 		}
// 	}

// 	// Update the consistent hash map to remove the server's virtual nodes and shard associations
// 	// chm.RemoveServer(strconv.Itoa(serverID))

// 	// Update serverConfigs to reflect removal
// 	for i, serverConfig := range configData.Servers {
// 		if serverConfig.ID == serverID {
// 			configData.Servers = append(configData.Servers[:i], configData.Servers[i+1:]...)
// 			break // Assuming server IDs are unique, can break after finding
// 		}
// 	}

// 	return true
// }

// Randomly chooses a server to remove
// func chooseRandomServer() string {
// 	if len(configData.Servers) == 0 {
// 		return ""
// 	}
// 	index := rand.Intn(len(configData.Servers))
// 	return configData.Servers[index].Hostname
// }

// func removeServersEndpoint(w http.ResponseWriter, r *http.Request) {
// 	var req RemoveRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
// 		return
// 	}

// 	// Sanity check: If the provided server list is longer than the number of servers to remove
// 	if len(req.Servers) > req.N {
// 		json.NewEncoder(w).Encode(RemoveResponse{
// 			Message: map[string]interface{}{"<Error>": "Length of server list is more than removable instances"},
// 			Status:  "failure",
// 		})
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}

// 	// Track removed servers for response
// 	removedServers := make([]string, 0, len(req.Servers))

// 	// Remove specified servers
// 	for _, serverName := range req.Servers {
// 		if removeServerInstance(serverName) {
// 			removedServers = append(removedServers, serverName)
// 		}
// 	}

// 	// If additional servers need to be randomly removed
// 	additionalRemovalsNeeded := req.N - len(removedServers)
// 	for additionalRemovalsNeeded > 0 {
// 		// Choose a server randomly and remove it
// 		if serverName := chooseRandomServer(); serverName != "" {
// 			if removeServerInstance(serverName) {
// 				removedServers = append(removedServers, serverName)
// 				additionalRemovalsNeeded--
// 			}
// 		}
// 	}

// 	response := RemoveResponse{
// 		Message: map[string]interface{}{
// 			"N":       len(configData.Servers) - len(removedServers),
// 			"servers": removedServers,
// 		},
// 		Status: "successful",
// 	}
// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusOK)
// 	if err := json.NewEncoder(w).Encode(response); err != nil {
// 		log.Printf("Failed to send response: %v", err)
// 		// Handle error in sending response if necessary.
// 	}
// }

// cleanupServers stops and removes all server containers
func cleanupServers() {
	fmt.Println("Cleaning up server instances...")

	servers := []int{}
	for _, mapTConfig := range mapTConfigs {
		contains := false
		for _, server := range servers {
			if server == mapTConfig.ServerID {
				contains = true
				break
			}
		}
		if !contains {
			servers = append(servers, mapTConfig.ServerID)
		}
	}

	for _, server := range servers {
		stopCmd := exec.Command("sudo", "docker", "stop", fmt.Sprintf("Server%d", server))
		removeCmd := exec.Command("sudo", "docker", "rm", fmt.Sprintf("Server%d", server))

		if err := stopCmd.Run(); err != nil {
			fmt.Printf("Failed to stop server '%d': %v", server, err)
		}
		if err := removeCmd.Run(); err != nil {
			fmt.Printf("Failed to remove server '%d': %v", server, err)
		}
	}
}

func main() {
	buildServerInstance()

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
	// http.HandleFunc("/add", addServersEndpoint)
	// http.HandleFunc("/rm", removeServersEndpoint)

	fmt.Println("Load Balancer running on port 5000")
	log.Fatal(http.ListenAndServe(":5000", nil))

	// Waiting for cleanup to be done before exiting
	<-cleanupDone
}
