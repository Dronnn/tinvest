package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/Dronnn/tinvest/internal/config"
	"github.com/Dronnn/tinvest/internal/render"
)

// version is overridden at release time via
// -ldflags "-X main.version=v0.x.y".
var version = "dev"

type versionData struct {
	Version       string `json:"version"`
	Contract      string `json:"contract"`
	SchemaVersion string `json:"schema_version"`
	Go            string `json:"go"`
}

func (a *app) versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print CLI, contract, and schema versions",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			data := versionData{
				Version:       version,
				Contract:      render.Contract,
				SchemaVersion: render.SchemaVersion,
				Go:            runtime.Version(),
			}
			// version must never fail on config problems, so only the flag
			// and the environment pick the output mode here.
			explicit := firstNonEmpty(a.flags.Output, os.Getenv(config.EnvOutput))
			if explicit != "" && explicit != "json" && explicit != "table" {
				return a.fail("json", render.UsageError(fmt.Sprintf("invalid output format %q (want json or table)", explicit)), render.NewMeta("", "", 0))
			}
			mode := render.Mode(explicit, os.Stdout)
			if mode == "table" {
				fmt.Printf("tinvest   %s\ncontract  %s\nschema    %s\ngo        %s\n",
					data.Version, data.Contract, data.SchemaVersion, data.Go)
				return nil
			}
			return render.WriteJSON(os.Stdout, render.Success(data, render.NewMeta("", "", 0)))
		},
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
