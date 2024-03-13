package main

import (
	"sync"

	"github.com/Sarita-Singh/galaxyDB/loadbalancer/internal/consistenthashmap"
)

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

type AddRequest struct {
	N         int                 `json:"n"`
	NewShards []ShardTConfig      `json:"new_shards"`
	Servers   map[string][]string `json:"servers"`
}

type AddResponseSuccess struct {
	N       int    `json:"N"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type AddResponseFailed struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type RemoveRequest struct {
	N       int      `json:"n"`
	Servers []string `json:"servers"`
}

type RemoveResponseSuccess struct {
	Message map[string]interface{} `json:"message"`
	Status  string                 `json:"status"`
}

type RemoveResponseFailed struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type ServerConfigPayload struct {
	Schema SchemaConfig `json:"schema"`
	Shards []string     `json:"shards"`
}
