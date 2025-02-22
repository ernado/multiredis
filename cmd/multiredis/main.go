package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type RedisInstance struct {
	Port int
}

type Config struct {
	BaseDir   string                   `yaml:"base_dir"`
	Instances map[string]RedisInstance `yaml:"instances"`
}

func newRootCommand() *cobra.Command {
	var args struct {
		Port   int
		Config string
	}
	cmd := &cobra.Command{
		Use:   "multiredis",
		Short: "Manage multiple redis instances",
		RunE: func(_ *cobra.Command, _ []string) error {
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&args.Config, "config", "/etc/multiredis.yaml", "config file")
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
