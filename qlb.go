package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var globalTransport = &http.Transport{
	// Tweak these if needed, e.g. timeouts, TLS, etc.
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: config.Insecure,
	},
	MaxConnsPerHost: 1000,
	// Example custom timeouts, connection limits, etc.
	MaxIdleConns:        1000,
	MaxIdleConnsPerHost: 1000,
	IdleConnTimeout:     90 * time.Second,
}

func main() {
	fmt.Println("Quick Load Balancer starting up...")
	LoadConfig()
	Listen()
}

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
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodConnect {
				HandleConnection(w, r)
			} else {
				HandleHTTP(w, r)
			}
		}),
	}

	log.Println("Starting proxy on " + config.ListenAddr + ":" + strconv.Itoa(config.ListenPort))
	log.Fatal(proxy.ListenAndServe())
}

func HandleConnection(w http.ResponseWriter, r *http.Request) {
	var match = GetMatch(r)
	var downstreamServer string
	if match == "" {
		log.Println("Required header not provided: " + strings.Join(config.TrackingHeaders, ", "))
		downstreamServer = GetRandomServer()
	} else {
		downstreamServer = GetDownstreamServer(match)
	}

	destConn, err := tls.Dial("tcp", downstreamServer, &tls.Config{
		InsecureSkipVerify: config.Insecure,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		log.Println("Error connecting to downstream server: ", err)
		return
	}
	defer destConn.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		log.Println("Error hijacking connection: ", err)
		return
	}

	defer clientConn.Close()

	_, err = fmt.Fprintf(clientConn, "HTTP/1.1 200 Connection Established\r\n\r\n")
	if err != nil {
		log.Println("Error writing to client connection: ", err)
		return
	}

	go io.Copy(destConn, clientConn)
	io.Copy(clientConn, destConn)
}

func HandleHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Println("Error creating request: ", err)
		return
	}
	req.Header = r.Header.Clone()

	// Use a custom HTTP client with the above transport
	client := &http.Client{
		Transport: globalTransport,
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		log.Println("Error sending request: ", err)
		return
	}
	defer resp.Body.Close()

	// Copy headers and status code
	for k, v := range resp.Header {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
	w.WriteHeader(resp.StatusCode)

	io.Copy(w, resp.Body)
}
