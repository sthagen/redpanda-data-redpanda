// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shadow",
		Args:  cobra.NoArgs,
		Short: "Manage Redpanda Shadow Links",
		Long: `Manage Redpanda Shadow Links.

Shadowing is Redpanda's enterprise-grade disaster recovery solution that
establishes asynchronous, offset-preserving replication between two distinct
Redpanda clusters. A cluster is able to create a dedicated client that
continuously replicates source cluster data, including offsets, timestamps, and
cluster metadata.
`,
	}
	cmd.AddCommand(
		newShadowConfigCommand(fs, p),
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newFailoverCommand(fs, p),
		newDescribeCommand(fs, p),
		newStatusCommand(fs, p),
		newListCommand(fs, p),
		newUpdateCommand(fs, p),
	)
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	return cmd
}
