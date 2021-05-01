package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

func main() {
	location := flag.String("location", "/metrics", "metrics location")
	listen := flag.String("listen", "0.0.0.0:9141", "address to listen on")
	timeout := flag.Int64("timeout", 30, "timeout for connection to zk servers, in seconds")
	zkhosts := flag.String("zk-hosts", "", "comma separated list of zk servers, e.g. '10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181'")
	zktlsauth := flag.Bool("zk-tls-auth", false, "zk tls client authentication")
	zktlscert := flag.String("zk-tls-auth-cert", "", "cert for zk tls client authentication")
	zktlskey := flag.String("zk-tls-auth-key", "", "key for zk tls client authentication")

	flag.Parse()

	var clientCert *tls.Certificate
	if *zktlsauth {
		if *zktlscert == "" || *zktlskey == "" {
			log.Fatal("-zk-tls-auth-cert and -zk-tls-auth-key flags are required when -zk-tls-auth is true")
		}
		_clientCert, err := tls.LoadX509KeyPair(*zktlscert, *zktlskey)
		if err != nil {
			log.Fatalf("fatal: can't load keypair %s, %s: %v", *zktlskey, *zktlscert, err)
		}
		clientCert = &_clientCert
	}

	hosts := strings.Split(*zkhosts, ",")
	if len(hosts) == 0 {
		log.Fatal("fatal: no target zookeeper hosts specified, exiting")
	}

	log.Printf("info: zookeeper hosts: %v", hosts)

	options := Options{
		Timeout:    *timeout,
		Hosts:      hosts,
		Location:   *location,
		Listen:     *listen,
		ClientCert: clientCert,
	}

	log.Printf("info: serving metrics at %s%s", *listen, *location)
	serveMetrics(&options)
}

type Options struct {
	Timeout    int64
	Hosts      []string
	Location   string
	Listen     string
	ClientCert *tls.Certificate
}

var versionRE = regexp.MustCompile(`^([0-9]+\.[0-9]+\.[0-9]+).*$`)

var metricNameReplacer = strings.NewReplacer("-", "_", ".", "_")

// open tcp connections to zk nodes, send 'mntr' and return result as a map
func getMetrics(options *Options) map[string]string {
	metrics := map[string]string{}

	for _, h := range options.Hosts {
		hostLabel := fmt.Sprintf("zk_host=%q", h)
		zkUp := fmt.Sprintf("zk_up{%s}", hostLabel)
		zkRuok := fmt.Sprintf("zk_ruok{%s}", hostLabel)
		metrics[zkUp] = "0"
		metrics[zkRuok] = "0"

		res, err := sendZookeeperCmd(h, "ruok", options)
		if err != nil || res[0] != "imok" {
			log.Print(err)
			continue
		}

		metrics[zkUp] = "1"
		metrics[zkRuok] = "1"

		res, err = sendZookeeperCmd(h, "mntr", options)
		if err != nil {
			log.Print(err)
			continue
		}

		// skip instance if it in a leader only state and doesnt serving client requets
		if res[0] == "This ZooKeeper instance is not currently serving requests" {
			metrics[fmt.Sprintf("zk_server_leader{%s}", hostLabel)] = "1"
			continue
		}

		// split each line into key-value pair
		for _, l := range res {
			l = strings.Replace(l, "\t", " ", -1)
			kv := strings.Split(l, " ")

			switch kv[0] {
			case "zk_server_state":
				zkLeader := fmt.Sprintf("zk_server_leader{%s}", hostLabel)
				if kv[1] == "leader" {
					metrics[zkLeader] = "1"
				} else {
					metrics[zkLeader] = "0"
				}

			case "zk_version":
				version := versionRE.ReplaceAllString(kv[1], "$1")

				metrics[fmt.Sprintf("zk_version{%s,version=%q}", hostLabel, version)] = "1"

			case "zk_peer_state":
				metrics[fmt.Sprintf("zk_peer_state{%s,state=%q}", hostLabel, kv[1])] = "1"

			case "": // noop on empty string

			default:
				metrics[fmt.Sprintf("%s{%s}", metricNameReplacer.Replace(kv[0]), hostLabel)] = kv[1]
			}
		}
	}

	return metrics
}

func dial(host string, timeout time.Duration, clientCert *tls.Certificate) (net.Conn, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("warning: cannot resolve zk hostname '%s': %s", host, err)
	}
	dialer := net.Dialer{Timeout: timeout}
	if clientCert == nil {
		return dialer.Dial("tcp", tcpaddr.String())
	} else {
		return tls.DialWithDialer(&dialer, "tcp", tcpaddr.String(), &tls.Config{
			Certificates:       []tls.Certificate{*clientCert},
			InsecureSkipVerify: true,
		})
	}
}

func sendZookeeperCmd(host, cmd string, options *Options) ([]string, error) {
	timeout := time.Duration(options.Timeout) * time.Second
	conn, err := dial(host, timeout, options.ClientCert)
	if err != nil {
		return nil, fmt.Errorf("warning: cannot connect to %s: %v", host, err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		return nil, fmt.Errorf("warning: failed to send '%s' to '%s': %s", cmd, host, err)
	}

	res, err := ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("warning: failed read '%s' response from '%s': %s", cmd, host, err)
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("warning: empty '%s' response from '%s'", cmd, host)
	}

	lines := strings.Split(string(res), "\n")
	if strings.Contains(lines[0], "is not executed because it is not in the whitelist.") {
		return nil, fmt.Errorf("warning: %s command isn't allowed at %s, see '4lw.commands.whitelist' ZK config parameter", cmd, host)
	}
	return lines, nil
}

// serve zk metrics at chosen address and url
func serveMetrics(options *Options) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		for k, v := range getMetrics(options) {
			fmt.Fprintf(w, "%s %s\n", k, v)
		}
	}

	http.HandleFunc(options.Location, handler)

	if err := http.ListenAndServe(options.Listen, nil); err != nil {
		log.Fatalf("fatal: shutting down exporter: %s", err)
	}
}
