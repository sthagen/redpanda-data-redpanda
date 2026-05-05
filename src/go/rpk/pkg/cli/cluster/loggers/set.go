// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package loggers

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// NewSetCommand returns the cluster loggers set command.
func NewSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		nodeID        int
		level         string
		expirySeconds int
	)
	cmd := &cobra.Command{
		Use:   "set [LOGGERS...]",
		Short: "Set broker logger's log level",
		Long: `Set broker logger's log level.

This command temporarily changes a broker logger's log level. Each Redpanda
broker has many loggers, and each can be individually changed. Any change
to a logger persists for a limited amount of time, so as to ensure you do
not accidentally enable debug logging permanently.

It is optional to specify a logger; if you do not, this command will prompt
from the set of available loggers.

The special logger "all" enables all loggers. Alternatively, you can specify
many loggers at once.

This command accepts loggers that it does not know of to ensure you can
independently update your redpanda installations from rpk. The success or
failure of enabling each logger is individually printed.

Use 'rpk cluster loggers list' to display available loggers.
`,
		ValidArgsFunction: func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return defaultLoggers, cobra.ShellCompDirectiveNoFileComp
		},
		Run: func(cmd *cobra.Command, loggers []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			cl, err = cl.ForBroker(cmd.Context(), nodeID)
			out.MaybeDie(err, "unable to resolve client for broker %d: %v", nodeID, err)

			RunSet(cmd.Context(), cl, fs, loggers, level, expirySeconds)
		},
	}

	cmd.Flags().StringVarP(&level, "level", "l", "debug", "Log level to set (error, warn, info, debug, trace)")
	cmd.Flags().IntVarP(&expirySeconds, "expiry-seconds", "e", 300, "Seconds to persist this log level override before redpanda reverts to its previous settings (if 0, persist until shutdown)")
	cmd.Flags().IntVar(&nodeID, "node-id", 0, "Broker to apply the log level change on")
	cobra.MarkFlagRequired(cmd.Flags(), "node-id")

	return cmd
}

// RunSet executes the set-loggers body against an already-built admin
// client. It is shared between `rpk cluster loggers set` and the deprecated
// `rpk redpanda admin config log-level set` shim, which builds its client
// from the legacy --host flag instead of --node-id.
func RunSet(ctx context.Context, cl *rpadmin.AdminAPI, fs afero.Fs, loggers []string, level string, expirySeconds int) {
	availableLoggers := DiscoverLoggers(ctx, cl, fs)

	switch len(loggers) {
	case 0:
		choices := append([]string{"all"}, availableLoggers...)
		pick, err := out.Pick(choices, "Which logger would you like to set (all selects everything)?")
		out.MaybeDie(err, "unable to pick logger: %v", err)
		if pick == "all" {
			loggers = availableLoggers
		} else {
			loggers = []string{pick}
		}

	case 1:
		if loggers[0] == "all" {
			loggers = availableLoggers
		}
	}

	type result struct {
		logger string
		err    error
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	var (
		mu      sync.Mutex
		results []result
	)
	for _, logger := range loggers {
		g.Go(func() error {
			err := cl.SetLogLevel(gctx, logger, level, expirySeconds)
			mu.Lock()
			results = append(results, result{logger, err})
			mu.Unlock()
			return nil
		})
	}
	g.Wait()

	var (
		failures  []result
		successes []string
	)
	for _, r := range results {
		if r.err != nil {
			failures = append(failures, r)
		} else {
			successes = append(successes, r.logger)
		}
	}
	if len(successes) > 0 {
		sort.Strings(successes)
		tw := out.NewTable("SUCCESS")
		for _, success := range successes {
			tw.Print(success)
		}
		tw.Flush()
	}
	if len(failures) > 0 {
		sort.Slice(failures, func(i, j int) bool { return failures[i].logger < failures[j].logger })
		if len(successes) > 0 {
			fmt.Println()
		}
		tw := out.NewTable("FAILURE", "ERROR")
		for _, f := range failures {
			tw.Print(f.logger, f.err)
		}
		tw.Flush()
		os.Exit(1)
	}
}
