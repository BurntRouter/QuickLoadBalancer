package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

// Config represents the configuration structure.
type Config struct {
	ListenAddr         string
	ListenPort         int
	SSLEnabled         bool
	SSLCert            string
	SSLKey             string
	Insecure           bool
	BackendServers     []string
	TrackingHeaders    []string
	TrackingValue      string
	Algo               string        // "random" or "leastconn"
	UnhealthyCooldown  time.Duration // Duration to exclude unhealthy servers (no longer used)
	InactivityDuration time.Duration // Duration after which inactive servers are removed
}

// dict represents a mapping for a match with its associated downstream server and last connection time.
type dict struct {
	lastConnected    time.Time
	downstreamServer string
}

// serverInfo holds information about each backend server.
type serverInfo struct {
	totalConnections int
	lastConnected    time.Time
	mu               sync.RWMutex
}

// Global variables
var (
	config             Config
	dataMap            = make(map[string]*dict)
	dataMu             sync.RWMutex
	serversInfo        = make(map[string]*serverInfo)
	serversInfoMu      sync.RWMutex
	randSrc            = rand.New(rand.NewSource(time.Now().UnixNano()))
	globalTransport    *http.Transport
	inactivityDuration time.Duration
)

func main() {
	fmt.Println("Reverse Proxy starting up...")
	LoadConfig()
	initializeServersInfo()
	initializeTransport()
	go monitorInactivity()
	Listen()
}

// LoadConfig loads the configuration from config.toml.
func LoadConfig() {
	var ConfigData Config
	if _, err := toml.DecodeFile("./config.toml", &ConfigData); err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
		os.Exit(1)
	}
	// Optionally validate right here
	if err := Validate(ConfigData); err != nil {
		fmt.Printf("Error validating config: %s\n", err)
		os.Exit(1)
	} else {
		config = ConfigData
	}
	inactivityDuration = config.InactivityDuration
}

// Validate validates the loaded configuration.
func Validate(c Config) error {
	var errs []string

	// Validate backend servers
	for _, ds := range c.BackendServers {
		u, err := url.Parse(ds)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Downstream server %q is not a valid URL: %v", ds, err))
			continue
		}

		// Check scheme
		if u.Scheme != "http" && u.Scheme != "https" {
			errs = append(errs, fmt.Sprintf("Downstream server %q must start with http:// or https://", ds))
			continue
		}

		// Check hostname
		if u.Hostname() == "" {
			errs = append(errs, fmt.Sprintf("Downstream server %q must have a hostname or IP", ds))
			continue
		}
	}

	// Validate listen address
	if c.ListenAddr == "" {
		errs = append(errs, "Listen address must be provided")
	}

	// Validate listen port
	if c.ListenPort < 1 || c.ListenPort > 65535 {
		errs = append(errs, fmt.Sprintf("Listen port %d is invalid, must be between 1 and 65535", c.ListenPort))
	}

	// Validate SSL cert and key
	if c.SSLEnabled {
		if c.SSLCert == "" {
			errs = append(errs, "SSL enabled but no SSL cert provided")
		} else {
			if _, err := os.Stat(c.SSLCert); err != nil {
				errs = append(errs, fmt.Sprintf("SSL cert file %q not found", c.SSLCert))
			}
		}
		if c.SSLKey == "" {
			errs = append(errs, "SSL enabled but no SSL key provided")
		} else {
			if _, err := os.Stat(c.SSLKey); err != nil {
				errs = append(errs, fmt.Sprintf("SSL key file %q not found", c.SSLKey))
			}
		}
	}

	// Validate algo
	if c.Algo == "" {
		errs = append(errs, "Algorithm must be provided")
	} else if c.Algo != "random" && c.Algo != "leastconn" {
		errs = append(errs, fmt.Sprintf("Algorithm %q is not supported", c.Algo))
	}

	// Validate InactivityDuration
	if c.InactivityDuration <= 0 {
		errs = append(errs, "InactivityDuration must be greater than zero")
	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + StringJoinWithPrefix(errs, "\n  - "))
	}
	return nil
}

// StringJoinWithPrefix joins strings with a separator.
func StringJoinWithPrefix(strs []string, sep string) string {
	var sb strings.Builder
	for i, s := range strs {
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteString(s)
	}
	return sb.String()
}

// initializeServersInfo initializes the serversInfo map.
func initializeServersInfo() {
	serversInfoMu.Lock()
	defer serversInfoMu.Unlock()
	for _, server := range config.BackendServers {
		serversInfo[server] = &serverInfo{
			totalConnections: 0,
			lastConnected:    time.Now(),
		}
	}
}

// initializeTransport sets up the global HTTP transport.
func initializeTransport() {
	globalTransport = &http.Transport{
		// Reuse your existing TLS configuration
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.Insecure,
		},
		MaxConnsPerHost:     1000,
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
}

// monitorInactivity periodically checks for inactive servers and removes them.
func monitorInactivity() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		<-ticker.C
		removeInactiveServers()
	}
}

// removeInactiveServers removes servers that have been inactive for InactivityDuration.
func removeInactiveServers() {
	serversInfoMu.Lock()
	defer serversInfoMu.Unlock()

	currentTime := time.Now()
	for server, info := range serversInfo {
		info.mu.RLock()
		lastActive := info.lastConnected
		info.mu.RUnlock()
		if currentTime.Sub(lastActive) > inactivityDuration {
			log.Printf("Removing server %s due to inactivity for %v", server, inactivityDuration)
			delete(serversInfo, server)

			// Remove associated matches
			dataMu.Lock()
			for match, entry := range dataMap {
				if entry.downstreamServer == server {
					delete(dataMap, match)
				}
			}
			dataMu.Unlock()
		}
	}
}

// AddData adds a new match with its downstream server to the dataMap.
func AddData(match, downstreamServer string) {
	dataMu.Lock()
	defer dataMu.Unlock()
	dataMap[match] = &dict{
		lastConnected:    time.Now(),
		downstreamServer: downstreamServer,
	}
	// Update serverInfo
	serversInfoMu.RLock()
	if info, exists := serversInfo[downstreamServer]; exists {
		info.mu.Lock()
		info.totalConnections++
		info.lastConnected = time.Now()
		info.mu.Unlock()
	}
	serversInfoMu.RUnlock()
}

// UpdateData updates the lastConnected time for a given match.
func UpdateData(match string) {
	dataMu.Lock()
	defer dataMu.Unlock()
	if entry, exists := dataMap[match]; exists {
		entry.lastConnected = time.Now()
		// Update serverInfo
		serversInfoMu.RLock()
		if info, exists := serversInfo[entry.downstreamServer]; exists {
			info.mu.Lock()
			info.lastConnected = time.Now()
			info.mu.Unlock()
		}
		serversInfoMu.RUnlock()
	}
}

// GetMatch constructs the match string based on request headers.
func GetMatch(r *http.Request) string {
	pattern := config.TrackingValue
	headers := config.TrackingHeaders

	for _, header := range headers {
		value := r.Header.Get(header)
		if value == "" {
			// If any required header isn’t provided, return empty
			return ""
		}
		// Replace all occurrences of {Header} with its value
		placeholder := "{" + header + "}"
		pattern = strings.ReplaceAll(pattern, placeholder, value)
	}

	return pattern
}

// GetDownstreamServer retrieves the downstream server for a given match.
// If not found, selects a new server based on the configured algorithm.
func GetDownstreamServer(match string) string {
	dataMu.RLock()
	entry, exists := dataMap[match]
	dataMu.RUnlock()

	if exists {
		UpdateData(match)
		return entry.downstreamServer
	}

	// If match not found, select a new downstream server
	server := GetNewDownstreamServer()
	AddData(match, server)
	return server
}

// GetNewDownstreamServer selects a downstream server based on the configured algorithm,
// excluding any servers marked as unhealthy.
func GetNewDownstreamServer() string {
	switch config.Algo {
	case "random":
		return GetRandomServer()
	case "leastconn":
		return GetLeastConnectedServer()
	default:
		log.Fatalf("Invalid algorithm specified in config: %s", config.Algo)
		return ""
	}
}

// GetRandomServer selects a random downstream server, ignoring health status.
func GetRandomServer() string {
	servers := filterHealthyServers(config.BackendServers)
	if len(servers) == 0 {
		log.Fatal("No downstream servers configured")
	}

	selectedServer := servers[randSrc.Intn(len(servers))]
	return selectedServer
}

// GetLeastConnectedServer selects the downstream server with the least total connections.
// If multiple servers have the same minimum connections, it selects one randomly among them.
func GetLeastConnectedServer() string {
	servers := filterHealthyServers(config.BackendServers)
	if len(servers) == 0 {
		log.Fatal("No downstream servers available")
	}

	var (
		leastConnectedServers []string
		minConnections        = int(^uint(0) >> 1) // Max int value
	)

	serversInfoMu.RLock()
	for _, server := range servers {
		if info, exists := serversInfo[server]; exists {
			info.mu.RLock()
			count := info.totalConnections
			info.mu.RUnlock()
			if count < minConnections {
				minConnections = count
				leastConnectedServers = []string{server}
			} else if count == minConnections {
				leastConnectedServers = append(leastConnectedServers, server)
			}
		}
	}
	serversInfoMu.RUnlock()

	if len(leastConnectedServers) == 0 {
		// Fallback to random server if no servers are tracked
		return GetRandomServer()
	}

	// If multiple servers have the same least connections, select one randomly
	selectedServer := leastConnectedServers[randSrc.Intn(len(leastConnectedServers))]
	return selectedServer
}

// filterHealthyServers returns all servers, ignoring health status.
func filterHealthyServers(servers []string) []string {
	return servers
}

// HandleHTTP handles incoming HTTP requests and forwards them to backend servers.
func HandleHTTP(w http.ResponseWriter, r *http.Request) {
	match := GetMatch(r)
	var backendServer string
	if match == "" {
		backendServer = GetRandomServer()
	} else {
		backendServer = GetDownstreamServer(match)
	}

	// Increment total connections for the selected backend server
	serversInfoMu.RLock()
	if info, exists := serversInfo[backendServer]; exists {
		info.mu.Lock()
		info.totalConnections++
		info.lastConnected = time.Now()
		info.mu.Unlock()
	}
	serversInfoMu.RUnlock()

	// Parse the backend server URL
	parsedBackendURL, err := url.Parse(backendServer)
	if err != nil {
		http.Error(w, "Invalid backend server URL", http.StatusInternalServerError)
		log.Printf("Invalid backend server URL %s: %v", backendServer, err)
		return
	}

	// Merge the request URI with the backend server URL
	parsedBackendURL.Path = singleJoiningSlash(parsedBackendURL.Path, r.URL.Path)
	parsedBackendURL.RawQuery = r.URL.RawQuery

	backendURL := parsedBackendURL.String()

	// Create a new request to send to the backend server
	req, err := http.NewRequest(r.Method, backendURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Println("Error creating request:", err)
		return
	}
	req.Header = r.Header.Clone()

	// Modify headers for reverse proxy
	addReverseProxyHeaders(req, r)

	// Use the global transport
	client := &http.Client{
		Transport: globalTransport,
	}

	// Send the request to the backend server
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error forwarding request to backend %s: %v", backendServer, err)
		handleBackendFailure(backendServer)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	// Copy headers from the backend response to the client response
	copyHeaders(w.Header(), resp.Header)

	// Write the status code to the client
	w.WriteHeader(resp.StatusCode)

	// Copy the response body to the client
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Println("Error copying response body:", err)
	}
}

// handleBackendFailure logs the failure but does not mark the server as unhealthy.
func handleBackendFailure(server string) {
	log.Printf("Error forwarding request to backend %s. Continuing to send traffic to it.", server)
	// Optionally implement retry logic or alerting here.
}

// copyHeaders copies headers from source to destination, excluding hop-by-hop headers.
func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		// Skip hop-by-hop headers
		if isHopHeader(key) {
			continue
		}
		for _, v := range values {
			dst.Add(key, v)
		}
	}
}

// addReverseProxyHeaders adds or modifies headers specific to reverse proxying.
func addReverseProxyHeaders(req *http.Request, originalReq *http.Request) {
	// Set the Host header to the backend server's host
	req.Host = req.URL.Host

	// Add X-Forwarded-For header
	originalIP := strings.Split(originalReq.RemoteAddr, ":")[0]
	if prior, ok := originalReq.Header["X-Forwarded-For"]; ok {
		originalIP = strings.Join(prior, ", ") + ", " + originalIP
	}
	req.Header.Set("X-Forwarded-For", originalIP)

	// Add X-Forwarded-Proto header
	if originalReq.TLS != nil {
		req.Header.Set("X-Forwarded-Proto", "https")
	} else {
		req.Header.Set("X-Forwarded-Proto", "http")
	}

	// Add any custom tracking headers
	for _, header := range config.TrackingHeaders {
		if value := originalReq.Header.Get(header); value != "" {
			req.Header.Set(header, value)
		}
	}
}

// isHopHeader determines if a header is a hop-by-hop header and should not be forwarded.
func isHopHeader(header string) bool {
	hopHeaders := []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	}

	header = http.CanonicalHeaderKey(header)
	for _, h := range hopHeaders {
		if h == header {
			return true
		}
	}
	return false
}

// singleJoiningSlash is a helper function to join paths without duplicating slashes.
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// Listen sets up the HTTP server and starts listening for incoming requests.
func Listen() {
	var tlsConfig *tls.Config
	if config.SSLEnabled {
		cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
		if err != nil {
			log.Fatalf("Failed to load SSL certificates: %v", err)
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	proxy := &http.Server{
		Addr:      config.ListenAddr + ":" + strconv.Itoa(config.ListenPort),
		TLSConfig: tlsConfig,
		Handler:   http.HandlerFunc(HandleHTTP), // Directly handle HTTP requests
	}

	log.Println("Starting reverse proxy on " + config.ListenAddr + ":" + strconv.Itoa(config.ListenPort))
	if config.SSLEnabled {
		log.Fatal(proxy.ListenAndServeTLS("", ""))
	} else {
		log.Fatal(proxy.ListenAndServe())
	}
}
