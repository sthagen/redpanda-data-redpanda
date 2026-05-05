// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package brokers contains commands to manage Redpanda cluster brokers
// (decommission, recommission, and decommission progress).
package brokers

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the cluster brokers command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "Manage Redpanda cluster brokers",
		Args:  cobra.ExactArgs(0),
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		NewDecommissionBroker(fs, p),
		NewDecommissionBrokerStatus(fs, p),
		NewRecommissionBroker(fs, p),
	)
	return cmd
}
