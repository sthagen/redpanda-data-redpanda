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
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func uninstallCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall the Redpanda Check plugin",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			check, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("check")
			if !pluginExists {
				out.Exit("The Redpanda Check managed plugin is not installed!")
			}
			ops, anyFailed := check.Uninstall(true)
			tw := out.NewTable("PATH", "MESSAGE")
			defer func() {
				tw.Flush()
				if anyFailed {
					os.Exit(1)
				}
			}()
			for _, o := range ops {
				tw.Print(o.Path, o.Message)
			}
		},
	}
	return cmd
}
