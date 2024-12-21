package main

import (
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	data   []dict
	dataMu sync.RWMutex
)

type dict struct {
	match            string
	lastConnected    time.Time
	downstreamServer string
}

func AddData(match, downstreamServer string) {
	dataMu.Lock()
	defer dataMu.Unlock()
	data = append(data, dict{match, time.Now(), downstreamServer})
}

func UpdateData(match string) {
	for i := 0; i < len(data); i++ {
		if data[i].match == match {
			data[i].lastConnected = time.Now()
		}
	}
}

func GetMatch(r *http.Request) string {
	// pattern might look like "{Server-IP}:{Port}"
	pattern := config.TrackingValue
	// headers might look like ["Server-IP", "Port"]
	headers := config.TrackingHeaders

	for _, header := range headers {
		value := r.Header.Get(header)
		if value == "" {
			// If any required header isn’t provided, return empty
			return ""
		}
		// Replace all occurrences of {Server-IP}, {Port}, etc.
		pattern = strings.ReplaceAll(pattern, "{"+header+"}", value)
	}

	return pattern
}

func GetDownstreamServer(m string) string {
	dataMu.RLock()
	defer dataMu.RUnlock()
	for i := len(data) - 1; i >= 0; i-- {
		if data[i].match == m {
			UpdateData(m)
			return data[i].downstreamServer
		}
	}
	var server = GetNewDownstreamServer(m)
	AddData(m, server)
	return server
}

func GetNewDownstreamServer(m string) string {
	if config.Algo == "random" {
		return GetRandomServer()
	} else if config.Algo == "leastconn" {
		return GetLeastConnectedServer()
	} else {
		log.Fatal("Invalid algorithm specified in config.toml")
		return ""
	}
}

func GetRandomServer() string {
	var servers = config.DownstreamServers
	// Get length of servers and generate random number
	// Use random number to select server
	if len(servers) == 0 {
		log.Fatal("No downstream servers available")
		return ""
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomIndex := r.Intn(len(servers))
	return servers[randomIndex]
}

func GetLeastConnectedServer() string {
	// Get server with least connections
	// If all servers have same number of connections, return random server
	if len(config.DownstreamServers) == 0 {
		log.Fatal("No downstream servers available")
		return ""
	}

	var leastConnectedServer string
	minConnections := int(^uint(0) >> 1) // Max int value

	for _, server := range config.DownstreamServers {
		connections := getConnectionCount(server)
		if connections < minConnections {
			minConnections = connections
			leastConnectedServer = server
		}
	}

	if leastConnectedServer == "" {
		return GetRandomServer()
	}

	return leastConnectedServer
}

func getConnectionCount(server string) int {
	// Get connection count from data[] slice
	// If server not found, return 0
	count := 0
	for _, d := range data {
		if d.downstreamServer == server {
			count++
		}
	}
	return count
}

func OpenDB() {
	db, err := bolt.Open("data.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
