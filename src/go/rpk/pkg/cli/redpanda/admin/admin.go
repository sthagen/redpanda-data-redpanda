// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package admin provides a cobra command for the redpanda admin listener.
package admin

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/brokers"
	configcmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/partitions"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the redpanda admin command.
//
// The whole `rpk redpanda admin` tree is deprecated in favor of `rpk
// cluster` subcommands. We set Hidden and Deprecated directly here instead
// of using cobraext.DeprecateCmd because that helper recurses into children
// and would overwrite the per-leaf migration messages we already set.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "admin",
		Short:      "Talk to the Redpanda admin listener",
		Args:       cobra.ExactArgs(0),
		Hidden:     true,
		Deprecated: "use `rpk cluster` subcommands; see `rpk cluster brokers`, `rpk cluster info --detailed`, `rpk cluster config list --node-id`, and `rpk cluster loggers`",
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		brokers.NewCommand(fs, p),
		partitions.NewCommand(fs, p),
		configcmd.NewCommand(fs, p),
	)
	return cmd
}
