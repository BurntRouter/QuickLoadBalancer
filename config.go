package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	DownstreamServers []string
	Insecure          bool
	ListenAddr        string
	ListenPort        int
	SSLEnabled        bool
	SSLCert           string
	SSLKey            string
	Database          string
	Algo              string
	TrackingMode      string
	TrackingHeaders   []string
	TrackingValue     string
}

var config Config

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
}

func Validate(c Config) error {
	var errs []string

	//Validate downstream servers
	for _, ds := range c.DownstreamServers {
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
			errs = append(errs, fmt.Sprintf("Downstream server %q must have a hostname or ip", ds))
			continue
		}
	}

	//Validate listen address
	if c.ListenAddr == "" {
		errs = append(errs, "Listen address must be provided")
	}

	//Validate listen port
	if c.ListenPort < 1 || c.ListenPort > 65535 {
		errs = append(errs, fmt.Sprintf("Listen port %d is invalid, must be between 1 and 65535", c.ListenPort))
	}

	//Validate SSL cert and key
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

	//Validate database
	if c.Database == "" {
		errs = append(errs, "Database must be provided")
	} else if c.Database != "memory" && c.Database != "file" {
		errs = append(errs, fmt.Sprintf("Database %q is not supported", c.Database))
	}

	//Validate algo
	if c.Algo == "" {
		errs = append(errs, "Algorithm must be provided")
	} else if c.Algo != "roundrobin" && c.Algo != "random" && c.Algo != "leastconn" {
		errs = append(errs, fmt.Sprintf("Algorithm %q is not supported", c.Algo))
	}

	//Validate tracking mode
	if c.TrackingMode == "" {
		errs = append(errs, "Tracking mode must be provided")
	} else if c.TrackingMode != "ip" && c.TrackingMode != "header" {
		errs = append(errs, fmt.Sprintf("Tracking mode %q is not supported", c.TrackingMode))
	} else {
		if c.TrackingMode == "header" {
			if len(c.TrackingHeaders) == 0 {
				errs = append(errs, "Tracking mode is header but no headers provided")
			}
		}

		//Check if the tracking value contains all the headers
		if c.TrackingMode == "header" {
			for _, header := range c.TrackingHeaders {
				if !strings.Contains(c.TrackingValue, header) {
					errs = append(errs, fmt.Sprintf("Tracking value %q does not contain all the headers", c.TrackingValue))
				}
			}
		}

	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + StringJoinWithPrefix(errs, "\n  - "))
	}
	return nil
}

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
