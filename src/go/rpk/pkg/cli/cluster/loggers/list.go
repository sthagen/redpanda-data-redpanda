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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewListCommand returns the cluster loggers list command.
func NewListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var nodeID int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List loggers available on a Redpanda broker",
		Long: `List loggers available on a Redpanda broker.

Loggers are discovered by, in order:
  1. Running the local redpanda binary with --help-loggers (Linux only).
  2. Querying /v1/loggers on the Admin API.
  3. Falling back to a hardcoded list compiled into rpk.

Without --node-id, the request is sent to any broker. Use --node-id to query
a specific broker.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if cmd.Flags().Changed("node-id") {
				cl, err = cl.ForBroker(cmd.Context(), nodeID)
				out.MaybeDie(err, "unable to resolve broker %d: %v", nodeID, err)
			}

			tw := out.NewTable("LOGGER")
			defer tw.Flush()
			for _, l := range DiscoverLoggers(cmd.Context(), cl, fs) {
				tw.Print(l)
			}
		},
	}
	cmd.Flags().IntVar(&nodeID, "node-id", 0, "If set, query the given broker instead of any broker")
	return cmd
}
