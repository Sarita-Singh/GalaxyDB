package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

type configPayload struct {
	Schema schema   `json:"schema"`
	Shards []string `json:"shards"`
}

type schema struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

func homeEndpoint(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string)
	resp["message"] = fmt.Sprintf("Hello from Server: %s", os.Getenv("id"))
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("error in JSON marshal: %s", err)
	}
	w.Write(jsonResp)
}

func heartbeatEndpoint(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
}

func configEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body
	var reqBody configPayload
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}

	// fmt.Println("Columns:", reqBody.Schema.Columns)
	// fmt.Println("Data Types:", reqBody.Schema.Dtypes)
	// fmt.Println("Shards:", reqBody.Shards)

	var resMsg string
	serverId := fmt.Sprintf("Server%s", os.Getenv("id"))
	numberShards := len(reqBody.Shards)
	for i, shard := range reqBody.Shards {
		resMsg += fmt.Sprintf("%s:%s", serverId, shard)
		if i == numberShards-1 {
			resMsg += " configured"
		} else {
			resMsg += ", "
		}
	}

	// initialize the shard tables in server database
	for _, shard := range reqBody.Shards {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ( ", shard)
		for i, col := range reqBody.Schema.Columns {
			query += fmt.Sprintf("%s %s", col, reqBody.Schema.Dtypes[i])
			if i < len(reqBody.Schema.Columns)-1 {
				query += ", "
			}
		}
		query += ")"
		_, err = db.Exec(query)
		if err != nil {
			log.Fatalf("error creating table: %s", err)
		}
	}

	// Send response
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string)

	resp["message"] = resMsg
	resp["status"] = "success"
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("error in JSON marshal: %s", err)
	}
	w.Write(jsonResp)
}

func main() {
	var err error
	db, err = sql.Open("sqlite3", "/galaxy.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/home", homeEndpoint)
	http.HandleFunc("/heartbeat", heartbeatEndpoint)
	http.HandleFunc("/config", configEndpoint)

	fmt.Println("Starting server on port 5000")
	err = http.ListenAndServe(":5000", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		panic(err)
	}
}
