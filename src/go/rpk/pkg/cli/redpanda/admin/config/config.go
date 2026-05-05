// Package config contains commands to talk to Redpanda's admin config
// endpoints.
//
// This package is named config to avoid import overlap with the rpk
// config package.
package config

import (
	"encoding/json"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/loggers"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the config admin command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "config",
		Short:  "View or modify Redpanda configuration through the admin listener",
		Args:   cobra.ExactArgs(0),
		Hidden: true,
	}
	cmd.AddCommand(
		cobraext.DeprecateCmd(newPrintCommand(fs, p), "rpk cluster config list --node-id <ID>"),
		newLogLevelCommand(fs, p),
	)
	return cmd
}

func newPrintCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var host string
	cmd := &cobra.Command{
		Use:     "print",
		Aliases: []string{"dump", "list", "ls", "display"},
		Short:   "Display the current Redpanda configuration",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewHostClient(fs, p, host)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			conf, err := cl.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to request configuration: %v", err)

			marshaled, err := json.MarshalIndent(conf, "", "  ")
			out.MaybeDie(err, "unable to json encode configuration: %v", err)

			fmt.Println(string(marshaled))
		},
	}

	cmd.Flags().StringVar(&host, "host", "", "either a hostname or an index into rpk.admin_api.addresses config section to select the hosts to issue the request to")
	cobra.MarkFlagRequired(cmd.Flags(), "host")

	return cmd
}

// newLogLevelCommand is the deprecated parent for `admin config log-level
// set`. cobraext.DeprecateCmd walks children, so the inner set command also
// surfaces as deprecated/hidden.
func newLogLevelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log-level",
		Short: "Manage a broker's log level",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newLegacyLogLevelSet(fs, p),
	)
	return cobraext.DeprecateCmd(cmd, "rpk cluster loggers")
}

// newLegacyLogLevelSet preserves the pre-deprecation flag surface of
// `admin config log-level set` (--host, --help-loggers) and adapts to the
// new `cluster/loggers` implementation. We keep this thin wrapper so
// existing automation against the deprecated command still runs during the
// migration window; the new `rpk cluster loggers set` uses --node-id.
func newLegacyLogLevelSet(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		host          string
		level         string
		expirySeconds int
		helpLoggers   bool
	)
	cmd := &cobra.Command{
		Use:   "set [LOGGERS...]",
		Short: "Set broker logger's log level",
		Run: func(cmd *cobra.Command, args []string) {
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(prof)

			if helpLoggers {
				adminHost := host
				if adminHost == "" && len(prof.AdminAPI.Addresses) > 0 {
					adminHost = prof.AdminAPI.Addresses[0]
				}
				if adminHost == "" {
					return
				}
				cl, err := adminapi.NewHostClient(fs, prof, adminHost)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)
				tw := out.NewTable("LOGGER")
				defer tw.Flush()
				for _, l := range loggers.DiscoverLoggers(cmd.Context(), cl, fs) {
					tw.Print(l)
				}
				return
			}

			if host == "" {
				out.Die("required flag \"host\" not set")
			}
			cl, err := adminapi.NewHostClient(fs, prof, host)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)
			loggers.RunSet(cmd.Context(), cl, fs, args, level, expirySeconds)
		},
	}
	cmd.Flags().BoolVar(&helpLoggers, "help-loggers", false, "Display the list of available loggers")
	cmd.Flags().StringVarP(&level, "level", "l", "debug", "Log level to set (error, warn, info, debug, trace)")
	cmd.Flags().IntVarP(&expirySeconds, "expiry-seconds", "e", 300, "Seconds to persist this log level override before redpanda reverts to its previous settings (if 0, persist until shutdown)")
	cmd.Flags().StringVar(&host, "host", "", "Either a hostname or an index into rpk.admin_api.addresses config section to select the hosts to issue the request to")
	return cmd
}
