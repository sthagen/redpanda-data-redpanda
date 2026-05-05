// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		includeDecom bool
		detailed     bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List the brokers in your cluster",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]cluster.BrokerDetail{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(prof)

			err = cluster.RunBrokerInfo(cmd.Context(), fs, prof, f, cmd.OutOrStdout(), includeDecom)
			out.MaybeDie(err, "%v", err)
		},
	}
	cmd.Flags().BoolVarP(&includeDecom, "include-decommissioned", "d", false, "If true, include decommissioned brokers")
	// --detailed is preserved as a hidden no-op for legacy automation: the
	// new path always renders the DISK SPACE section when admin returns it.
	cmd.Flags().BoolVar(&detailed, "detailed", false, "")
	cmd.Flags().MarkHidden("detailed")
	p.InstallFormatFlag(cmd)
	return cmd
}
