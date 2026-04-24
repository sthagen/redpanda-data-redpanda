// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"
	"io"
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type cloudAuthRow struct {
	Name           string `json:"name" yaml:"name"`
	Kind           string `json:"kind" yaml:"kind"`
	Organization   string `json:"organization" yaml:"organization"`
	OrganizationID string `json:"organization_id" yaml:"organization_id"`
	Current        bool   `json:"current" yaml:"current"`
}

func printCloudAuthList(f config.OutFormatter, rows []cloudAuthRow, w io.Writer) {
	if isText, _, rendered, err := f.Format(rows); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, rendered)
		return
	}
	tw := out.NewTableTo(w, "NAME", "KIND", "ORGANIZATION", "ORGANIZATION-ID")
	defer tw.Flush()
	for _, r := range rows {
		name := r.Name
		if r.Current {
			name += "*"
		}
		tw.Print(name, r.Kind, r.Organization, r.OrganizationID)
	}
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List rpk cloud authentications",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]cloudAuthRow{}); ok {
				out.Exit(h)
			}

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				return
			}

			sort.Slice(y.CloudAuths, func(i, j int) bool {
				// First by organization name, then by org ID, then by name.
				l, r := y.CloudAuths[i], y.CloudAuths[j]
				return l.Organization < r.Organization ||
					(l.Organization == r.Organization && (l.OrgID < r.OrgID ||
						(l.OrgID == r.OrgID && l.Name < r.Name)))
			})

			rows := make([]cloudAuthRow, 0, len(y.CloudAuths))
			for i := range y.CloudAuths {
				a := &y.CloudAuths[i]
				rows = append(rows, cloudAuthRow{
					Name:           a.Name,
					Kind:           a.Kind,
					Organization:   a.Organization,
					OrganizationID: a.OrgID,
					Current:        a.OrgID == y.CurrentCloudAuthOrgID && a.Kind == y.CurrentCloudAuthKind,
				})
			}

			printCloudAuthList(f, rows, cmd.OutOrStdout())
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
