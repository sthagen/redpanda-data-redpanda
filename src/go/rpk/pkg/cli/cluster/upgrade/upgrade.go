// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package upgrade contains commands to manage Redpanda cluster version
// deferred ("unfinalized") upgrades.
package upgrade

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Manage Redpanda cluster version upgrades",
		Long: `Check and finalize major-version upgrades. When the cluster configuration
'features_auto_finalization' is disabled, an upgrade stays pending after all
brokers are on the new version, so you can downgrade while soak-testing.
`,
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		newFinalizeCommand(fs, p),
		newStatusCommand(fs, p),
	)
	return cmd
}
