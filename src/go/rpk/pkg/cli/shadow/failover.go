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
	"fmt"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newFailoverCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm bool
		all       bool
		topic     string
	)
	cmd := &cobra.Command{
		Use:   "failover [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Failover a Redpanda Shadow Link",
		Long:  `Failover a Redpanda Shadow Link or a specific topic`,
		Run: func(cmd *cobra.Command, args []string) {
			if !all && topic == "" {
				out.Die("either --all or --topic must be provided")
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			linkName := args[0]
			if !noConfirm {
				sl, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
					Name: linkName,
				}))
				out.MaybeDie(err, "unable to get Redpanda Shadow Link %q: %v", linkName, err)
				printOverview(sl.Msg.GetShadowLink())
				var confirmed bool
				if all {
					confirmed, err = out.Confirm("Are you sure you want to failover all topics for Shadow Link %q?", linkName)
				} else {
					confirmed, err = out.Confirm("Are you sure you want to failover the topic %q for Shadow Link %q?", topic, linkName)
				}
				out.MaybeDie(err, "unable to confirm Shadow Link failover: %v", err)
				if !confirmed {
					out.Exit("Command execution canceled.")
				}
			}
			_, err = cl.ShadowLinkService().FailOver(cmd.Context(), connect.NewRequest(&adminv2.FailOverRequest{
				Name:            linkName,
				ShadowTopicName: topic,
			}))
			out.MaybeDie(err, "unable to failover Shadow Link: %v", err)

			fmt.Printf(`Successfully initiated the Fail Over for Shadow Link %q. To check the status, run:
  rpk shadow status %[1]s
`, linkName)
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().BoolVar(&all, "all", false, "Failover all shadow links")
	cmd.Flags().StringVar(&topic, "topic", "", "Specific topic to failover. If --all is not set, at least a topic must be provided")

	cmd.MarkFlagsMutuallyExclusive("all", "topic")
	return cmd
}
