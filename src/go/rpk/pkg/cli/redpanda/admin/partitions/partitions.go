// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package partitions contains commands to talk to the Redpanda's admin partitions
// endpoints.
package partitions

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type partitionResponse struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
	IsLeader  bool   `json:"is_leader" yaml:"is_leader"`
}

// NewCommand returns the partitions admin command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "partitions",
		Short:  "View and configure Redpanda partitions through the admin listener",
		Args:   cobra.ExactArgs(0),
		Hidden: true,
	}
	cmd.AddCommand(
		cobraext.DeprecateCmd(newListCommand(fs, p), "rpk cluster partitions list --node-ids <ID>"),
	)
	return cmd
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var leaderOnly bool
	cmd := &cobra.Command{
		Use:     "list [BROKER ID]",
		Aliases: []string{"ls"},
		Short:   "List the partitions in a broker in the cluster",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]partitionResponse{}); ok {
				out.Exit(h)
			}

			brokerID, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if brokerID < 0 {
				out.Die("invalid negative broker id %v", brokerID)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var m kadm.Metadata
			m, err = adm.Metadata(context.Background())
			out.MaybeDie(err, "unable to request metadata: %v", err)

			var resp []partitionResponse
			for _, t := range m.Topics.Sorted() {
				for _, pt := range t.Partitions.Sorted() {
					for _, rs := range pt.Replicas {
						if int(rs) == brokerID {
							isLeader := int(pt.Leader) == brokerID
							if isLeader || !leaderOnly {
								resp = append(resp, partitionResponse{
									Topic:     t.Topic,
									Partition: pt.Partition,
									IsLeader:  isLeader,
								})
							}
						}
					}
				}
			}

			printAdminPartitionList(f, resp, cmd.OutOrStdout())
		},
	}

	cmd.Flags().BoolVarP(&leaderOnly, "leader-only", "l", false, "print the partitions on broker which are leaders")
	p.InstallFormatFlag(cmd)

	return cmd
}

func printAdminPartitionList(f config.OutFormatter, data []partitionResponse, w io.Writer) {
	if isText, _, s, err := f.Format(data); !isText {
		out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
		fmt.Fprintln(w, s)
		return
	}
	tw := out.NewTableTo(w, "Topic", "Partition", "Is-Leader")
	defer tw.Flush()
	for _, p := range data {
		tw.Print(p.Topic, p.Partition, p.IsLeader)
	}
}
