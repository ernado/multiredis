package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type RedisInstance struct {
	Port      int    `yaml:"port"`
	ReplicaOf string `yaml:"replicaOf"`

	Sentinel RedisInstanceSentinel `yaml:"sentinel"`
}

type RedisInstanceSentinel struct {
	StartPort int `yaml:"startPort"`
	Replicas  int `yaml:"replicas"`
}

type RedisCluster struct {
	StartPort int `yaml:"startPort"`
	Replicas  int `yaml:"replicas"`
}

type Config struct {
	BaseDir   string                   `yaml:"baseDir"`
	Instances map[string]RedisInstance `yaml:"instances"`
	Clusters  map[string]RedisCluster  `yaml:"clusters"`
}

type RedisConfig struct {
	Bind []string
	Port int

	ClusterEnabled     bool
	ClusterConfigFile  string
	ClusterNodeTimeout int

	ReplicaOfHost string
	ReplicaOfPort int
}

func (r RedisConfig) String() string {
	var b strings.Builder

	b.WriteString("bind ")
	for i, bind := range r.Bind {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(bind)
	}
	b.WriteString("\n")

	b.WriteString("port ")
	b.WriteString(fmt.Sprintf("%d", r.Port))
	b.WriteString("\n")

	if r.ReplicaOfHost != "" {
		b.WriteString("replicaof ")
		b.WriteString(r.ReplicaOfHost)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", r.ReplicaOfPort))
		b.WriteString("\n")
	}

	if r.ClusterEnabled {
		b.WriteString("cluster-enabled yes\n")
		b.WriteString("cluster-config-file ")
		b.WriteString(r.ClusterConfigFile)
		b.WriteString("\n")
		b.WriteString("cluster-node-timeout ")
		b.WriteString(fmt.Sprintf("%d", r.ClusterNodeTimeout))
		b.WriteString("\n")
	}

	return b.String()
}

type SentinelMonitor struct {
	Name      string
	Host      string
	Port      int
	Agreement int

	DownAfterMilliseconds int
	FailoverTimeout       int
	ParallelSyncs         int
}

type RedisSentinelConfig struct {
	Port     int
	Monitors []SentinelMonitor
}

func (r RedisSentinelConfig) String() string {
	var b strings.Builder

	b.WriteString("port ")
	b.WriteString(fmt.Sprintf("%d", r.Port))
	b.WriteString("\n")

	for _, monitor := range r.Monitors {
		b.WriteString("sentinel monitor ")
		b.WriteString(monitor.Name)
		b.WriteString(" ")
		b.WriteString(monitor.Host)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", monitor.Port))
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", monitor.Agreement))
		b.WriteString("\n")
		b.WriteString("sentinel down-after-milliseconds ")
		b.WriteString(monitor.Name)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", monitor.DownAfterMilliseconds))
		b.WriteString("\n")
		b.WriteString("sentinel failover-timeout ")
		b.WriteString(monitor.Name)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", monitor.FailoverTimeout))
		b.WriteString("\n")
		b.WriteString("sentinel parallel-syncs ")
		b.WriteString(monitor.Name)
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%d", monitor.ParallelSyncs))
		b.WriteString("\n")
	}

	return b.String()
}

// prefixWriter is a writer that writes a prefix before each line.
type prefixWriter struct {
	io.Writer

	prefix     string
	needPrefix bool
	mux        *sync.Mutex
}

func newPrefixWriter(mux *sync.Mutex, prefix string) *prefixWriter {
	return &prefixWriter{
		Writer: os.Stderr,
		prefix: prefix,
		mux:    mux,
	}
}

func (w prefixWriter) Write(p []byte) (n int, err error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.needPrefix {
		if _, err := w.Writer.Write([]byte(w.prefix)); err != nil {
			return 0, err
		}
	}

	w.needPrefix = false
	const newline = '\n'

	for i, elem := range bytes.Split(p, []byte{newline}) {
		if i > 0 {
			if _, err := w.Writer.Write([]byte{newline}); err != nil {
				return 0, err
			}
		}
		if len(elem) > 0 {
			if _, err := w.Writer.Write([]byte(w.prefix)); err != nil {
				return 0, err
			}
			w.needPrefix = false
		}
		if _, err := w.Writer.Write(elem); err != nil {
			return 0, err
		}
		w.needPrefix = true
	}

	return len(p), nil
}

func newRootCommand() *cobra.Command {
	var args struct {
		Port   int
		Config string
	}
	cmd := &cobra.Command{
		Use:   "multiredis",
		Short: "Manage multiple redis instances",
		RunE: func(cobraCommand *cobra.Command, _ []string) error {
			data, err := os.ReadFile(args.Config)
			if err != nil {
				return errors.Wrap(err, "read config file")
			}

			var config Config
			if err := yaml.Unmarshal(data, &config); err != nil {
				return errors.Wrap(err, "unmarshal config")
			}

			if err := os.MkdirAll(config.BaseDir, 0700); err != nil {
				return errors.Wrap(err, "create base dir")
			}

			mux := &sync.Mutex{}
			const host = "127.0.0.1"

			ctx := cobraCommand.Context()
			shutdownContext, shutdownCancel := signal.NotifyContext(ctx, os.Interrupt)
			defer shutdownCancel()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			g, ctx := errgroup.WithContext(ctx)
			go func() {
				<-shutdownContext.Done()
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
					cancel()
				}
			}()
			for name, instance := range config.Instances {
				for i := 0; i < instance.Sentinel.Replicas; i++ {
					instanceDir := filepath.Join(config.BaseDir, name, "sentinel")
					if err := os.MkdirAll(instanceDir, 0700); err != nil {
						return errors.Wrap(err, "create instance dir")
					}

					configPath := filepath.Join(instanceDir, "sentinel.conf")
					cfg := RedisSentinelConfig{
						Port: instance.Sentinel.StartPort + i,
						Monitors: []SentinelMonitor{
							{
								Name:                  name,
								Host:                  host,
								Port:                  instance.Port,
								Agreement:             1,
								DownAfterMilliseconds: 30000,
								FailoverTimeout:       180000,
								ParallelSyncs:         1,
							},
						},
					}

					cfgData := []byte(cfg.String())
					if err := os.WriteFile(configPath, cfgData, 0600); err != nil {
						return errors.Wrap(err, "write config file")
					}

					cmd := exec.CommandContext(ctx, "redis-sentinel", configPath)
					prefix := fmt.Sprintf("[%s.sentinel.%d] ", name, i)
					cmd.Stdout = newPrefixWriter(mux, prefix)
					cmd.Stderr = newPrefixWriter(mux, prefix)
					cmd.Dir = instanceDir

					g.Go(func() error {
						defer func() {
							fmt.Printf("%sstoppped\n", prefix)
						}()
						return cmd.Run()
					})
				}
				g.Go(func() error {
					instanceDir := filepath.Join(config.BaseDir, name)
					if err := os.MkdirAll(instanceDir, 0700); err != nil {
						return errors.Wrap(err, "create instance dir")
					}

					configPath := filepath.Join(instanceDir, "redis.conf")
					cfg := RedisConfig{
						Bind: []string{host},
						Port: instance.Port,
					}

					if instance.ReplicaOf != "" {
						cfg.ReplicaOfHost = host
						cfg.ReplicaOfPort = config.Instances[instance.ReplicaOf].Port
					}

					cfgData := []byte(cfg.String())
					if err := os.WriteFile(configPath, cfgData, 0600); err != nil {
						return errors.Wrap(err, "write config file")
					}

					cmd := exec.CommandContext(ctx, "redis-server", configPath)
					prefix := fmt.Sprintf("[%s] ", name)
					cmd.Stdout = newPrefixWriter(mux, prefix)
					cmd.Stderr = newPrefixWriter(mux, prefix)
					cmd.Dir = instanceDir

					defer func() {
						fmt.Printf("%sstoppped\n", prefix)
					}()

					return cmd.Run()
				})
			}

			for clusterName, cluster := range config.Clusters {
				var nodes []string
				for i := 0; i < cluster.Replicas; i++ {
					instanceName := fmt.Sprintf("%s.%d", clusterName, i)
					instancePort := cluster.StartPort + i
					instanceAddr := net.JoinHostPort(host, fmt.Sprintf("%d", instancePort))
					nodes = append(nodes, instanceAddr)

					instanceDir := filepath.Join(config.BaseDir, clusterName, fmt.Sprintf("%d", i))
					if err := os.MkdirAll(instanceDir, 0700); err != nil {
						return errors.Wrap(err, "create instance dir")
					}

					g.Go(func() error {
						configPath := filepath.Join(instanceDir, "redis.conf")
						cfg := RedisConfig{
							Bind:               []string{host},
							Port:               instancePort,
							ClusterEnabled:     true,
							ClusterConfigFile:  "nodes.conf",
							ClusterNodeTimeout: 5000,
						}
						prefix := fmt.Sprintf("[%s] ", instanceName)

						cfgData := []byte(cfg.String())
						if err := os.WriteFile(configPath, cfgData, 0600); err != nil {
							return errors.Wrap(err, "write config file")
						}

						cmd := exec.CommandContext(ctx, "redis-server", configPath)
						cmd.Stdout = newPrefixWriter(mux, prefix)
						cmd.Stderr = newPrefixWriter(mux, prefix)
						cmd.Dir = instanceDir
						defer func() {
							fmt.Printf("%sstoppped\n", prefix)
						}()
						if err := cmd.Run(); err != nil {
							return errors.Wrapf(err, "start redis instance %s", instanceName)
						}

						return nil
					})
				}

				g.Go(func() error {
					time.Sleep(time.Second)

					cmdArgs := []string{
						"--cluster",
						"create",
					}
					cmdArgs = append(cmdArgs, nodes...)
					cmdArgs = append(cmdArgs, "--cluster-replicas", "1")
					cmdArgs = append(cmdArgs, "--cluster-yes")

					cmd := exec.CommandContext(ctx, "redis-cli", cmdArgs...)

					buf := &bytes.Buffer{}
					cmd.Stdout = buf
					cmd.Stderr = buf

					if err := cmd.Run(); err != nil {
						if strings.Contains(buf.String(), "is not empty") {
							// Already created.
							return nil
						}
						return errors.Wrap(err, "create redis cluster")
					}

					return nil
				})
			}

			if err := g.Wait(); err != nil {
				return errors.Wrap(err, "run redis instances")
			}

			return nil
		},
	}
	f := cmd.Flags()
	f.StringVarP(&args.Config, "config", "c", "/etc/multiredis.yaml", "config file")
	return cmd
}

func run() error {
	cmd := newRootCommand()
	return cmd.Execute()
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
