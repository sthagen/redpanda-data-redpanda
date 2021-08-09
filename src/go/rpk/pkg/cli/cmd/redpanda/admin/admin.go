// Copyright 2021 Vectorized, Inc.
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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin/brokers"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

// NewCommand returns the redpanda admin command.
func NewCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Talk to the Redpanda admin listener.",
		Args:  cobra.ExactArgs(0),
	}

	var (
		configFile     string
		hosts          []string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	cmd.PersistentFlags().StringVar(
		&configFile,
		"config",
		"",
		"rpk config file, if not set the file will be searched for"+
			" in the default locations",
	)
	configClosure := common.FindConfigFile(mgr, &configFile)

	cmd.PersistentFlags().StringSliceVar(
		&hosts,
		"hosts",
		[]string{},
		"A comma-separated list of Admin API addresses (<IP>:<port>)."+
			" You must specify one for each node.",
	)
	hostsClosure := func() []string {
		return common.DeduceAdminApiAddrs(
			configClosure,
			&hosts,
		)
	}

	common.AddAdminAPITLSFlags(
		cmd,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)
	tlsClosure := common.BuildAdminApiTLSConfig(
		fs,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
		configClosure,
	)

	cmd.AddCommand(
		brokers.NewCommand(hostsClosure, tlsClosure),
	)

	return cmd
}
