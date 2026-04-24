// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"fmt"
	"io"
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type profileListItem struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Current     bool   `json:"current" yaml:"current"`
}

func buildProfileList(y *config.RpkYaml) []profileListItem {
	items := make([]profileListItem, 0, len(y.Profiles))
	for _, p := range y.Profiles {
		items = append(items, profileListItem{
			Name:        p.Name,
			Description: p.Description,
			Current:     p.Name == y.CurrentProfile,
		})
	}
	return items
}

func printProfileList(f config.OutFormatter, items []profileListItem, w io.Writer) {
	if isText, _, t, err := f.Format(items); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := out.NewTableTo(w, "Name", "Description")
	defer tw.Flush()
	for _, item := range items {
		name := item.Name
		if item.Current {
			name += "*"
		}
		tw.Print(name, item.Description)
	}
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List rpk profiles",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]profileListItem{}); ok {
				out.Exit(h)
			}

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				return
			}

			sort.Slice(y.Profiles, func(i, j int) bool {
				return y.Profiles[i].Name < y.Profiles[j].Name
			})

			printProfileList(f, buildProfileList(y), cmd.OutOrStdout())
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
