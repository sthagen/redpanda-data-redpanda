// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"fmt"
	"io"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type pluginRow struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Installed   bool   `json:"installed" yaml:"installed"`
	Message     string `json:"message,omitempty" yaml:"message,omitempty"`
}

type localPluginRow struct {
	Name    string   `json:"name" yaml:"name"`
	Path    string   `json:"path" yaml:"path"`
	Shadows []string `json:"shadows,omitempty" yaml:"shadows,omitempty"`
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var local bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available plugins",
		Long: `List all available plugins.

By default, this command fetches the remote manifest and prints plugins
available for download. Any plugin that is already downloaded is prefixed with
an asterisk. If a locally installed plugin has a different sha256sum as the one
specified in the manifest, or if the sha256sum could not be calculated for the
local plugin, an additional message is printed.

You can specify --local to print all locally installed plugins, as well as
whether you have "shadowed" plugins (the same plugin specified multiple times).
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if local {
				if h, ok := f.Help([]localPluginRow{}); ok {
					out.Exit(h)
				}
			} else {
				if h, ok := f.Help([]pluginRow{}); ok {
					out.Exit(h)
				}
			}

			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			if local {
				installed.Sort()
				rows := buildLocalPluginList(installed)
				printLocalPluginList(f, rows, cmd.OutOrStdout())
				return
			}

			m, err := getManifest()
			out.MaybeDieErr(err)

			rows := buildPluginList(fs, installed, m.Plugins)
			printPluginList(f, rows, cmd.OutOrStdout())
		},
	}

	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&local, "local", "l", false, "List locally installed plugins and shadowed plugins")

	return cmd
}

func buildPluginList(fs afero.Fs, installed plugin.Plugins, entries []plugin.ManifestPlugin) []pluginRow {
	rows := make([]pluginRow, 0, len(entries))
	for _, entry := range entries {
		_, entrySha, _ := entry.PathShaForUser()

		row := pluginRow{
			Name:        entry.Name,
			Description: entry.Description,
		}

		p, exists := installed.Find(entry.Name)
		if exists {
			row.Installed = true
			sha, err := plugin.Sha256Path(fs, p.Path)
			if err != nil {
				row.Message = fmt.Sprintf("unable to calculate local binary sha256: %v", err)
			} else if sha != entrySha {
				row.Message = "local binary sha256 differs from manifest sha256"
			}
		}

		rows = append(rows, row)
	}
	return rows
}

func printPluginList(f config.OutFormatter, rows []pluginRow, w io.Writer) {
	if isText, _, t, err := f.Format(rows); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", t)
		return
	}
	tw := out.NewTableTo(w, "Name", "Description", "Message")
	defer tw.Flush()
	for _, r := range rows {
		name := r.Name
		if r.Installed {
			name = "*" + name
		}
		tw.Print(name, r.Description, r.Message)
	}
}

func buildLocalPluginList(installed plugin.Plugins) []localPluginRow {
	rows := make([]localPluginRow, 0, len(installed))
	for _, p := range installed {
		row := localPluginRow{
			Name:    p.FullName(),
			Path:    p.Path,
			Shadows: p.ShadowedPaths,
		}
		rows = append(rows, row)
	}
	return rows
}

func printLocalPluginList(f config.OutFormatter, rows []localPluginRow, w io.Writer) {
	if isText, _, t, err := f.Format(rows); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", t)
		return
	}
	tw := out.NewTableTo(w, "Name", "Path", "Shadows")
	defer tw.Flush()
	for _, r := range rows {
		if len(r.Shadows) == 0 {
			tw.Print(r.Name, r.Path, "")
			continue
		}
		tw.Print(r.Name, r.Path, r.Shadows[0])
		for _, shadowed := range r.Shadows[1:] {
			tw.Print("", "", shadowed)
		}
	}
}
