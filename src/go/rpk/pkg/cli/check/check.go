// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package check

import (
	"fmt"
	"os"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// injectProfileArgs adds admin API connection flags derived from the current
// rpk profile to pluginArgs, unless the user already passed them explicitly.
// This lets 'rpk check' work against remote clusters by inheriting admin_api
// and kafka_api settings from the active profile (same as other rpk commands).
func injectProfileArgs(p *config.Params, fs afero.Fs, pluginArgs []string) []string {
	cfg, err := p.Load(fs)
	if err != nil {
		zap.L().Debug("unable to load rpk config; skipping profile arg injection", zap.Error(err))
		return pluginArgs
	}
	profile := cfg.VirtualProfile()
	a := &profile.AdminAPI
	if len(a.Addresses) > 0 && !hasFlag(pluginArgs, "--admin-url") {
		pluginArgs = append(pluginArgs, "--admin-url", strings.Join(a.Addresses, ","))
	}
	if tls := a.TLS; tls != nil {
		if tls.TruststoreFile != "" && !hasFlag(pluginArgs, "--admin-tls-ca") {
			pluginArgs = append(pluginArgs, "--admin-tls-ca", tls.TruststoreFile)
		}
		if tls.CertFile != "" && !hasFlag(pluginArgs, "--admin-tls-cert") {
			pluginArgs = append(pluginArgs, "--admin-tls-cert", tls.CertFile)
		}
		if tls.KeyFile != "" && !hasFlag(pluginArgs, "--admin-tls-key") {
			pluginArgs = append(pluginArgs, "--admin-tls-key", tls.KeyFile)
		}
		if tls.InsecureSkipVerify && !hasFlag(pluginArgs, "--admin-tls-skip-verify") {
			pluginArgs = append(pluginArgs, "--admin-tls-skip-verify")
		}
	}
	if profile.KafkaAPI.SASL != nil {
		sasl := profile.KafkaAPI.SASL
		if sasl.User != "" && !hasFlag(pluginArgs, "--sasl-user") {
			pluginArgs = append(pluginArgs, "--sasl-user", sasl.User)
		}
		// Pass the SASL password via REDPANDA_SASL_PASSWORD on the plugin
		// subprocess rather than appending --sasl-password to argv, so it
		// can't leak through process listings (ps, /proc/<pid>/cmdline)
		// or the plugin-args debug log below. The plugin reads from this
		// env var as of redpanda-check v0.1.3 with the flag retained as
		// a fallback for explicit-flag callers.
		if sasl.Password != "" && os.Getenv("REDPANDA_SASL_PASSWORD") == "" {
			os.Setenv("REDPANDA_SASL_PASSWORD", sasl.Password)
		}
	}
	return pluginArgs
}

func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "check",
		Short:              "Run production readiness checks for a Redpanda deployment",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseCheckFlags(p, cmd, args)
			out.MaybeDie(err, "unable to parse flags: %v", err)

			// Help short-circuit. Done before profile-arg injection so an
			// injected --admin-url value can't make the auto-install
			// heuristic below think the user wants to run the plugin.
			if cmd.Flags().Changed("help") {
				cmd.Help()
				return
			}

			check, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("check")
			var pluginPath string
			if !pluginExists {
				// Decide whether to auto-install based on user-supplied args
				// (profile injection hasn't run yet). Anything beyond bare
				// help / version metadata is treated as intent to run the
				// plugin, including key=value flag forms like
				// --admin-url=<addr> that the previous heuristic missed.
				var wantsRun bool
				for _, arg := range pluginArgs {
					switch arg {
					case "--version":
						fmt.Println("cannot get check version: redpanda-check is not installed; run 'rpk check install'")
						cmd.Help()
						return
					case "--help", "-h":
						// metadata-only; not enough to trigger an install
					default:
						wantsRun = true
					}
				}
				if !wantsRun {
					cmd.Help()
					return
				}
				fmt.Fprintln(os.Stderr, "Downloading latest Redpanda Check")
				path, _, err := installCheck(cmd.Context(), fs, "latest")
				out.MaybeDie(err, "unable to install redpanda check: %v; you may install 'redpanda-check' manually", err)
				pluginPath = path
			} else {
				pluginPath = check.Path
				if !check.Managed {
					zap.L().Sugar().Warn("rpk is using a self-managed version of Redpanda Check. If you want rpk to manage check, use rpk check uninstall && rpk check install.")
				}
			}

			// Inject profile-derived connection info only when actually
			// running checks. Skip for --version (metadata only, no need
			// to set REDPANDA_SASL_PASSWORD on the rpk process env).
			if !contains(pluginArgs, "--version") {
				pluginArgs = injectProfileArgs(p, fs, pluginArgs)
			}

			zap.L().Debug("executing check plugin", zap.String("path", pluginPath), zap.Strings("args", pluginArgs))
			err = execFn(pluginPath, pluginArgs)
			out.MaybeDie(err, "unable to execute redpanda check plugin: %v", err)
		},
	}
	// Declared so it shows up in 'rpk check --help'. Pass-through is wired
	// in parseCheckFlags below; the actual print is handled by the plugin
	// (or by the no-plugin-installed branch in Run).
	cmd.Flags().Bool("version", false, "Print the installed redpanda-check plugin version")
	cmd.AddCommand(
		installCommand(fs),
		uninstallCommand(fs),
		upgradeCommand(fs),
	)
	return cmd
}

func parseCheckFlags(p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	f := cmd.Flags()
	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, err
	}
	zap.ReplaceGlobals(p.BuildLogger())
	if cobraext.LongFlagValue(args, f, "help", "h") == "true" && !contains(keepForPlugin, "--help") {
		keepForPlugin = append(keepForPlugin, "--help")
	}
	if cobraext.LongFlagValue(args, f, "version", "") == "true" && !contains(keepForPlugin, "--version") {
		keepForPlugin = append(keepForPlugin, "--version")
	}
	return keepForPlugin, nil
}

func hasFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag || strings.HasPrefix(a, flag+"=") {
			return true
		}
	}
	return false
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
