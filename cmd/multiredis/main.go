package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

type RedisInstance struct {
	Port      int    `yaml:"port"`
	ReplicaOf string `yaml:"replicaOf"`
}

type Config struct {
	BaseDir   string                   `yaml:"base_dir"`
	Instances map[string]RedisInstance `yaml:"instances"`
}

type RedisConfig struct {
	Bind []string
	Port int

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
			g, ctx := errgroup.WithContext(ctx)
			for name, instance := range config.Instances {
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

					cmd := exec.Command("redis-server", configPath)
					prefix := fmt.Sprintf("[%s] ", name)
					cmd.Stdout = newPrefixWriter(mux, prefix)
					cmd.Stderr = newPrefixWriter(mux, prefix)
					cmd.Dir = instanceDir

					return cmd.Run()
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
