package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sarita-Singh/galaxyDB/loadbalancer/internal/consistenthashmap"
)

const ServerDockerImageName = "galaxydb-server"
const DockerNetworkName = "galaxydb-network"
const ServerPort = 5000

var (
	schemaConfig  SchemaConfig
	shardTConfigs []ShardTConfig
	mapTConfigs   []MapTConfig
	serverIDs     []int
)

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

		serverIDs = append(serverIDs, serverID)
		spawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		configNewServerInstance(serverID, shardIDs, req.Schema)
	}

	for _, shard := range req.Shards {
		newShardTConfig := shard
		newShardTConfig.validIdx = 0
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
	w.WriteHeader(http.StatusOK)
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

func addServersHandler(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) < req.N {
		resp := AddResponseFailed{
			Message: "<Error> Number of new servers (n) is greater than newly added instances",
			Status:  "failure",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	serverIDsAdded := []int{}

	for rawServerName, shardIDs := range req.Servers {
		serverID := getServerID(rawServerName)
		serverIDsAdded = append(serverIDsAdded, serverID)
		for _, shardID := range shardIDs {
			mapTConfigs = append(mapTConfigs, MapTConfig{ShardID: shardID, ServerID: serverID})
		}

		serverIDs = append(serverIDs, serverID)
		spawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		configNewServerInstance(serverID, shardIDs, schemaConfig)
	}

	for _, shard := range req.NewShards {
		newShardTConfig := shard
		newShardTConfig.validIdx = 0
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

	addServerMessage := "Add "
	for index, server := range serverIDsAdded {
		addServerMessage = fmt.Sprintf("%sServer:%d", addServerMessage, server)
		if index == len(serverIDsAdded)-1 {
			continue
		} else if index == len(serverIDsAdded)-2 {
			addServerMessage += " and "
		} else {
			addServerMessage += ", "
		}
	}

	response := AddResponseSuccess{
		N:       len(serverIDs),
		Message: addServerMessage,
		Status:  "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func removeServersHandler(w http.ResponseWriter, r *http.Request) {
	var req RemoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) > req.N {
		resp := RemoveResponseFailed{
			Message: "<Error> Length of server list is more than removable instances",
			Status:  "failure",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	serverIDsRemoved := []int{}
	for _, serverName := range req.Servers {
		serverIDsRemoved = append(serverIDsRemoved, getServerID(serverName))
	}

	additionalRemovalsNeeded := req.N - len(serverIDsRemoved)
	for additionalRemovalsNeeded > 0 {
		if serverID := chooseRandomServerForRemoval(serverIDs, serverIDsRemoved); serverID != -1 {
			serverIDsRemoved = append(serverIDsRemoved, serverID)
			additionalRemovalsNeeded -= 1
		}
	}

	newMapTConfigs := []MapTConfig{}
	for _, mapTConfig := range mapTConfigs {
		isPresent := false
		for _, serverIDRemoved := range serverIDsRemoved {
			if mapTConfig.ServerID == serverIDRemoved {
				isPresent = true
				break
			}
		}
		if isPresent {
			for _, shardTConfig := range shardTConfigs {
				if shardTConfig.ShardID == mapTConfig.ShardID {
					shardTConfig.chm.RemoveServer(mapTConfig.ServerID)
				}
			}
		} else {
			newMapTConfigs = append(newMapTConfigs, mapTConfig)
		}
	}
	mapTConfigs = newMapTConfigs

	newServerIDs := []int{}
	for _, serverID := range serverIDs {
		isPresent := false
		for _, serverIDRemoved := range serverIDsRemoved {
			if serverIDRemoved == serverID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			newServerIDs = append(newServerIDs, serverID)
		}
	}
	serverIDs = newServerIDs

	serverNamesRemoved := []string{}
	for _, serverIDRemoved := range serverIDsRemoved {
		serverNameRemoved := fmt.Sprintf("Server%d", serverIDRemoved)
		removeServerInstance(serverNameRemoved)

		serverNamesRemoved = append(serverNamesRemoved, serverNameRemoved)
	}

	response := RemoveResponseSuccess{
		Message: map[string]interface{}{
			"N":       len(serverIDs),
			"servers": serverNamesRemoved,
		},
		Status: "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardIDsQueried := []string{}
	for _, shardTConfig := range shardTConfigs {
		if (shardTConfig.StudIDLow >= req.StudID.Low && shardTConfig.StudIDLow+shardTConfig.ShardSize <= req.StudID.High) ||
			(req.StudID.Low >= shardTConfig.StudIDLow && req.StudID.Low <= shardTConfig.StudIDLow+shardTConfig.ShardSize) ||
			(req.StudID.High >= shardTConfig.StudIDLow && req.StudID.High <= shardTConfig.StudIDLow+shardTConfig.ShardSize) {
			shardIDsQueried = append(shardIDsQueried, shardTConfig.ShardID)
		}
	}

	var studData []StudT

	for _, shardIDQueried := range shardIDsQueried {
		payload := ServerReadPayload{
			Shard:  shardIDQueried,
			StudID: req.StudID,
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			log.Fatalln("Error marshaling JSON: ", err)
			return
		}

		for _, shardTConfig := range shardTConfigs {
			if shardTConfig.ShardID == shardIDQueried {
				serverID := shardTConfig.chm.GetServerForRequest(getRandomID())

				resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(ServerPort)+"/read", "application/json", bytes.NewBuffer(payloadData))
				if err != nil {
					log.Println("Error reading from Server:", err)
					return
				}

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Println("Error reading response body:", err)
				}

				var respData ServerReadResponse
				json.Unmarshal(body, &respData)
				resp.Body.Close()

				studData = append(studData, respData.Data...)
			}
		}
	}

	response := ReadResponse{
		ShardsQueried: shardIDsQueried,
		Data:          studData,
		Status:        "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func WriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	for _, shardTConfig := range shardTConfigs {
		var studDataToWrite []StudT
		for _, studData := range req.Data {
			if studData.StudID >= shardTConfig.StudIDLow && studData.StudID <= shardTConfig.StudIDLow+shardTConfig.ShardSize {
				studDataToWrite = append(studDataToWrite, studData)
			}
		}

		if len(studDataToWrite) > 0 {
			shardTConfig.mutex.Lock()

			payload := ServerWritePayload{
				Shard:        shardTConfig.ShardID,
				Data:         studDataToWrite,
				CurrentIndex: shardTConfig.validIdx,
			}
			payloadData, err := json.Marshal(payload)
			if err != nil {
				log.Fatalln("Error marshaling JSON: ", err)
				return
			}

			for _, mapTConfig := range mapTConfigs {
				if mapTConfig.ShardID == shardTConfig.ShardID {
					resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", mapTConfig.ServerID))+":"+fmt.Sprint(ServerPort)+"/write", "application/json", bytes.NewBuffer(payloadData))
					if err != nil {
						log.Println("Error writing to Server:", err)
						return
					}

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						log.Println("Error reading response body:", err)
					}

					var respData ServerWriteResponse
					json.Unmarshal(body, &respData)
					resp.Body.Close()

					if respData.CurrentIndex != shardTConfig.validIdx+len(studDataToWrite) {
						log.Println("Error writing to Server: Invalid Index")
						return
					}
				}
			}

			shardTConfig.validIdx += len(studDataToWrite)
			shardTConfig.mutex.Unlock()
		}
	}

	response := WriteResponse{
		Status:  "success",
		Message: fmt.Sprintf("%d Data entries added", len(req.Data)),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func main() {
	buildServerInstance()

	sigs := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/add", addServersHandler)
	http.HandleFunc("/rm", removeServersHandler)
	http.HandleFunc("/read", readHandler)
	http.HandleFunc("/write", WriteHandler)

	server := &http.Server{Addr: ":5000", Handler: nil}

	go func() {
		<-sigs
		cleanupServers(serverIDs)
		server.Shutdown(context.Background())
		cleanupDone <- true
	}()

	log.Println("Load Balancer running on port 5000")
	err := server.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	}

	<-cleanupDone
	os.Exit(0)
}
