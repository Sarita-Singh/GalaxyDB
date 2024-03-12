package consistenthashmap

const (
	Slots = 512
	K     = 9
)

type ConsistentHashMap struct {
	virtualServers [Slots]int
}

func hashRequest(i int) int {
	return i*i + 2*i + 17
}

func hashVirtualServer(i, j int) int {
	return i*i + j*j + 2*j + 25
}

func (hm *ConsistentHashMap) Init() {
	for i := 0; i < Slots; i++ {
		hm.virtualServers[i] = -1
	}
}

func (hm *ConsistentHashMap) findEmptyServerSlot(hashValue int) int {
	slot := hashValue % Slots
	i := 0
	for hm.virtualServers[slot] != -1 && i <= Slots {
		slot = (slot + 1) % Slots
		i++
	}
	if i > Slots {
		return -1
	}
	return slot
}

func (hm *ConsistentHashMap) AddServer(serverID int) {
	for j := 0; j < K; j++ {
		slot := hm.findEmptyServerSlot(hashVirtualServer(serverID, j))
		hm.virtualServers[slot] = serverID
	}
}

func (hm *ConsistentHashMap) GetServerForRequest(requestID int) int {
	slot := hashRequest(requestID) % Slots
	i := 0
	for hm.virtualServers[slot] == -1 && i <= Slots {
		slot = (slot + 1) % Slots
		i++
	}
	if i > Slots {
		return -1
	}
	return hm.virtualServers[slot]
}

func (hm *ConsistentHashMap) RemoveServer(serverID int) {
	for j := 0; j < K; j++ {
		virtualServerHash := hashVirtualServer(serverID, j)
		slot := virtualServerHash % Slots
		if hm.virtualServers[slot] == serverID {
			hm.virtualServers[slot] = -1

			nextSlot := (slot + 1) % Slots
			for hm.virtualServers[nextSlot] == serverID {
				hm.virtualServers[nextSlot] = -1
				nextSlot = (nextSlot + 1) % Slots
			}
		}
	}
}

// package consistenthashmap

// import (
// 	"fmt"
// 	"hash/fnv"
// 	"sort"
// 	"strings"
// 	"sync"
// )

// const (
// 	Slots = 512
// 	K     = 9
// )

// type virtualNode struct {
// 	hash     int
// 	serverID string
// 	shardID  string
// }

// type Shard struct {
// 	ID       string
// 	Replicas []string
// }

// type ConsistentHashMap struct {
// 	shards       map[string]*Shard
// 	virtualNodes []virtualNode
// 	mutex        sync.Mutex
// }

// func NewConsistentHashMap() *ConsistentHashMap {
// 	return &ConsistentHashMap{
// 		shards: make(map[string]*Shard),
// 	}
// }

// func hashFunction(key string) int {
// 	hasher := fnv.New32a()
// 	hasher.Write([]byte(key))
// 	return int(hasher.Sum32()) % Slots
// }

// func (hm *ConsistentHashMap) AddShard(shardID string, serverIDs []string) {
// 	hm.mutex.Lock()
// 	defer hm.mutex.Unlock()

// 	shard, exists := hm.shards[shardID]
// 	if !exists {
// 		shard = &Shard{ID: shardID, Replicas: make([]string, 0)}
// 		hm.shards[shardID] = shard
// 	}
// 	shard.Replicas = append(shard.Replicas, serverIDs...)

// 	for _, serverID := range serverIDs {
// 		for i := 0; i < K; i++ {
// 			vnodeKey := fmt.Sprintf("%s-%s-%d", shardID, serverID, i)
// 			vnodeHash := hashFunction(vnodeKey)
// 			hm.virtualNodes = append(hm.virtualNodes, virtualNode{
// 				hash:     vnodeHash,
// 				serverID: serverID,
// 				shardID:  shardID,
// 			})
// 		}
// 	}

// 	sort.Slice(hm.virtualNodes, func(i, j int) bool {
// 		return hm.virtualNodes[i].hash < hm.virtualNodes[j].hash
// 	})
// }

// func (hm *ConsistentHashMap) AddServer(serverID string, shardIDs []string) {
// 	hm.mutex.Lock()
// 	defer hm.mutex.Unlock()

// 	for _, shardID := range shardIDs {
// 		hm.AddShard(shardID, []string{serverID})
// 	}
// }

// func (hm *ConsistentHashMap) RemoveServer(serverID string) {
// 	hm.mutex.Lock()
// 	defer hm.mutex.Unlock()

// 	newVirtualNodes := make([]virtualNode, 0)
// 	for _, vnode := range hm.virtualNodes {
// 		if vnode.serverID != serverID {
// 			newVirtualNodes = append(newVirtualNodes, vnode)
// 		}
// 	}
// 	hm.virtualNodes = newVirtualNodes

// 	for _, shard := range hm.shards {
// 		newReplicas := make([]string, 0)
// 		for _, replica := range shard.Replicas {
// 			if replica != serverID {
// 				newReplicas = append(newReplicas, replica)
// 			}
// 		}
// 		shard.Replicas = newReplicas
// 	}
// }

// func (hm *ConsistentHashMap) GetServerForKey(shardID, key string) string {
// 	hm.mutex.Lock()
// 	defer hm.mutex.Unlock()

// 	keyHash := hashFunction(key)
// 	var chosenServer string

// 	// Linear search for the first virtual node that can serve the key
// 	for _, vnode := range hm.virtualNodes {
// 		if vnode.hash >= keyHash {
// 			chosenServer = vnode.serverID
// 			break
// 		}
// 	}

// 	// If we haven't found a suitable server, wrap around the hash circle
// 	if chosenServer == "" && len(hm.virtualNodes) > 0 {
// 		chosenServer = hm.virtualNodes[0].serverID
// 	}

// 	return chosenServer
// }

// func (hm *ConsistentHashMap) RemoveShard(shardID string) {
// 	hm.mutex.Lock()
// 	defer hm.mutex.Unlock()

// 	delete(hm.shards, shardID)

// 	// Remove virtual nodes associated with the shard
// 	filteredNodes := hm.virtualNodes[:0]
// 	for _, vnode := range hm.virtualNodes {
// 		if !strings.HasPrefix(vnode.serverID, shardID+"-") {
// 			filteredNodes = append(filteredNodes, vnode)
// 		}
// 	}
// 	hm.virtualNodes = filteredNodes
// }

