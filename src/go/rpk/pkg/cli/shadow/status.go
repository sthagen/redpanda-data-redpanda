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
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type slStatusOptions struct {
	all      bool
	overview bool
	task     bool
	topic    bool
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var opts slStatusOptions
	cmd := &cobra.Command{
		Use:   "status [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Get the status of a Redpanda Shadow Link",
		Long: `Get the status of a Redpanda Shadow Link.

This command shows the current status of a Shadow Link, including the overall
state, task statuses, and per-topic replication progress. Use this command to
monitor replication health and track how closely shadow topics follow the source
cluster.

By default, the command displays all status sections. Use the flags to display
specific sections such as overview, task status, or topic status.
`,
		Example: `
Display the status of a Shadow Link:
  rpk shadow status my-shadow-link

Display specific sections:
  rpk shadow status my-shadow-link --print-overview --print-topic
`,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p) // TODO: remove, for now is there because we don't support it in cloud yet.

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			linkName := args[0]
			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link status %q: %v", linkName, handleConnectError(err, "get", linkName))

			opts.defaultOrAll()
			printShadowLinkStatus(link.Msg.GetShadowLink(), opts)
		},
	}
	cmd.Flags().BoolVarP(&opts.overview, "print-overview", "o", false, "Print the overview section")
	cmd.Flags().BoolVarP(&opts.task, "print-task", "k", false, "Print the task status section")
	cmd.Flags().BoolVarP(&opts.topic, "print-topic", "t", false, "Print the detailed topic status section")
	cmd.Flags().BoolVarP(&opts.all, "print-all", "a", false, "Print all sections")

	return cmd
}

// If no flags are set, default to overview and client sections.
func (o *slStatusOptions) defaultOrAll() {
	// We currently default to all sections until we have more fields to show.
	if o.all || (!o.overview && !o.task && !o.topic) {
		o.overview, o.task, o.topic = true, true, true
	}
}

func printShadowLinkStatus(link *adminv2.ShadowLink, opts slStatusOptions) {
	status := link.GetStatus()
	if link == nil || status == nil {
		fmt.Println("No status available")
		return
	}
	const (
		secOverview = "Overview"
		secTasks    = "Tasks"
		secTopics   = "Topics"
	)

	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secOverview: opts.overview,
			secTasks:    opts.task,
			secTopics:   opts.topic,
		})...,
	)

	sections.Add(secOverview, func() {
		printOverview(link)
	})

	sections.Add(secTasks, func() {
		printTaskStatus(status.GetTaskStatuses())
	})

	sections.Add(secTopics, func() {
		printTopicStatus(status.GetShadowTopics(), status.GetSyncedShadowTopicProperties())
	})
}

func printTaskStatus(tasks []*adminv2.ShadowLinkTaskStatus) {
	if len(tasks) == 0 {
		fmt.Println("No tasks to display right now.")
		return
	}
	t := out.NewTable("Name", "Broker_ID", "Shard", "State", "Reason")
	defer t.Flush()
	for _, task := range tasks {
		t.Print(task.GetName(), task.GetBrokerId(), task.GetShard(), strings.TrimPrefix(task.GetState().String(), "TASK_STATE_"), task.GetReason())
	}
}

func printTopicStatus(st []*adminv2.ShadowTopic, sp []string) {
	if len(st) == 0 {
		fmt.Println("No topics are being shadowed.")
		return
	}
	for _, topic := range st {
		var topicID string
		if topic.GetTopicId() != "" {
			topicID = fmt.Sprintf(", ID: %v", topic.GetTopicId())
		}
		fmt.Printf("Name: %s%s, State: %s\n", topic.GetName(), topicID, strings.TrimPrefix(topic.GetStatus().GetState().String(), "SHADOW_TOPIC_STATE_"))
		printPartitionTable(topic.GetStatus().GetPartitionInformation())
		fmt.Println()
	}
	if len(sp) > 0 {
		fmt.Println("Synced Shadow Topic Properties:")
		for _, prop := range sp {
			fmt.Printf("  - %s\n", prop)
		}
	}
}

func printPartitionTable(partitions []*adminv2.TopicPartitionInformation) {
	t := out.NewTable("", "Partition", "SRC_LSO", "SRC_HWM", "DST_HWM", "Lag")
	defer t.Flush()
	for _, p := range partitions {
		t.Print("", p.GetPartitionId(), p.GetSourceLastStableOffset(), p.GetSourceHighWatermark(), p.GetHighWatermark(), p.GetSourceHighWatermark()-p.GetHighWatermark())
	}
}
