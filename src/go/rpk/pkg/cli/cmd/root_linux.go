// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/redpanda"
)

func addPlatformDependentCmds(
	fs afero.Fs, mgr config.Manager, cmd *cobra.Command,
) {
	cmd.AddCommand(NewRedpandaCommand(fs, mgr, redpanda.NewLauncher()))
	cmd.AddCommand(NewDebugCommand(fs, mgr))

	cmd.AddCommand(NewTuneCommand(fs, mgr))
	cmd.AddCommand(NewCheckCommand(fs, mgr))
	cmd.AddCommand(NewIoTuneCmd(fs, mgr))
	cmd.AddCommand(NewStartCommand(fs, mgr, redpanda.NewLauncher()))
	cmd.AddCommand(NewStopCommand(fs, mgr))
	cmd.AddCommand(NewConfigCommand(fs, mgr))
	cmd.AddCommand(NewStatusCommand(fs, mgr))
	cmd.AddCommand(NewModeCommand(mgr))
}
