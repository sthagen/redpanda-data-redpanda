// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

type partitionMoveStatus struct {
	NamespaceTopic    string `json:"namespace_topic" yaml:"namespace_topic"`
	Partition         int    `json:"partition" yaml:"partition"`
	MovingFrom        []int  `json:"moving_from" yaml:"moving_from"`
	MovingTo          []int  `json:"moving_to" yaml:"moving_to"`
	CompletionPercent int    `json:"completion_percent" yaml:"completion_percent"`
	PartitionSize     int    `json:"partition_size" yaml:"partition_size"`
	BytesMoved        int    `json:"bytes_moved" yaml:"bytes_moved"`
	BytesRemaining    int    `json:"bytes_remaining" yaml:"bytes_remaining"`
}

type reconciliationOperation struct {
	Core        int    `json:"core" yaml:"core"`
	Type        string `json:"type" yaml:"type"`
	RetryNumber int    `json:"retry_number" yaml:"retry_number"`
	Revision    int    `json:"revision" yaml:"revision"`
	Status      string `json:"status" yaml:"status"`
}

type reconciliationNodeStatus struct {
	NodeID     int                       `json:"node_id" yaml:"node_id"`
	Operations []reconciliationOperation `json:"operations" yaml:"operations"`
}

type partitionReconciliation struct {
	NamespaceTopic string                     `json:"namespace_topic" yaml:"namespace_topic"`
	Partition      int                        `json:"partition" yaml:"partition"`
	NodeStatuses   []reconciliationNodeStatus `json:"node_statuses" yaml:"node_statuses"`
}

type moveStatusResponse struct {
	Movements       []partitionMoveStatus     `json:"movements" yaml:"movements"`
	Reconciliations []partitionReconciliation `json:"reconciliations,omitempty" yaml:"reconciliations,omitempty"`
}

func buildMoveStatuses(reconfigs []rpadmin.ReconfigurationsResponse) []partitionMoveStatus {
	statuses := make([]partitionMoveStatus, 0, len(reconfigs))
	for _, r := range reconfigs {
		var completion int
		if r.PartitionSize > 0 {
			completion = r.BytesMoved * 100 / r.PartitionSize
		}
		from := make([]int, 0, len(r.PreviousReplicas))
		for _, replica := range r.PreviousReplicas {
			from = append(from, replica.NodeID)
		}
		to := make([]int, 0, len(r.NewReplicas))
		for _, replica := range r.NewReplicas {
			to = append(to, replica.NodeID)
		}
		statuses = append(statuses, partitionMoveStatus{
			NamespaceTopic:    r.Ns + "/" + r.Topic,
			Partition:         r.PartitionID,
			MovingFrom:        from,
			MovingTo:          to,
			CompletionPercent: completion,
			PartitionSize:     r.PartitionSize,
			BytesMoved:        r.BytesMoved,
			BytesRemaining:    r.BytesLeft,
		})
	}
	return statuses
}

func buildReconciliations(reconfigs []rpadmin.ReconfigurationsResponse) []partitionReconciliation {
	recs := make([]partitionReconciliation, 0, len(reconfigs))
	for _, r := range reconfigs {
		nodes := make([]reconciliationNodeStatus, 0, len(r.ReconciliationStatuses))
		for _, s := range r.ReconciliationStatuses {
			ops := make([]reconciliationOperation, 0, len(s.Operations))
			for _, op := range s.Operations {
				ops = append(ops, reconciliationOperation{
					Core:        op.Core,
					Type:        op.Type,
					RetryNumber: op.RetryNumber,
					Revision:    op.Revision,
					Status:      op.Status,
				})
			}
			nodes = append(nodes, reconciliationNodeStatus{
				NodeID:     s.NodeID,
				Operations: ops,
			})
		}
		recs = append(recs, partitionReconciliation{
			NamespaceTopic: r.Ns + "/" + r.Topic,
			Partition:      r.PartitionID,
			NodeStatuses:   nodes,
		})
	}
	return recs
}

func buildMoveStatusResponse(reconfigs []rpadmin.ReconfigurationsResponse, includeReconciliations bool) moveStatusResponse {
	resp := moveStatusResponse{
		Movements: buildMoveStatuses(reconfigs),
	}
	if includeReconciliations {
		resp.Reconciliations = buildReconciliations(reconfigs)
	}
	return resp
}

func printMoveStatus(f config.OutFormatter, resp moveStatusResponse, human bool, w io.Writer) {
	if isText, _, t, err := f.Format(resp); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	sizeFn := func(size int) string {
		if human {
			return units.HumanSize(float64(size))
		}
		return strconv.Itoa(size)
	}

	const (
		secMove      = "Partition movements"
		secReconcile = "Reconciliation statuses"
	)
	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secMove:      true,
			secReconcile: resp.Reconciliations != nil,
		})...,
	)
	sections.Add(secMove, func() {
		tw := out.NewTableTo(w, "NAMESPACE-TOPIC", "PARTITION", "MOVING-FROM", "MOVING-TO", "COMPLETION-%", "PARTITION-SIZE", "BYTES-MOVED", "BYTES-REMAINING")
		defer tw.Flush()
		for _, s := range resp.Movements {
			tw.Print(
				s.NamespaceTopic,
				s.Partition,
				fmt.Sprint(s.MovingFrom),
				fmt.Sprint(s.MovingTo),
				s.CompletionPercent,
				sizeFn(s.PartitionSize),
				sizeFn(s.BytesMoved),
				sizeFn(s.BytesRemaining),
			)
		}
	})
	sections.Add(secReconcile, func() {
		for i, r := range resp.Reconciliations {
			if i > 0 {
				fmt.Fprintln(w)
			}
			fmt.Fprintf(w, "%s/%d\n", r.NamespaceTopic, r.Partition)
			tw := out.NewTableTo(w, "Node-id", "Core", "Type", "Retry-number", "Revision", "Status")
			for _, s := range r.NodeStatuses {
				row := []any{s.NodeID}
				for _, op := range s.Operations {
					row = append(row, op.Core, op.Type, op.RetryNumber, op.Revision, op.Status)
				}
				tw.Print(row...)
			}
			tw.Flush()
		}
	})
}

func newPartitionMovementsStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all        bool
		human      bool
		partitions []string
	)
	cmd := &cobra.Command{
		Use:   "move-status",
		Short: "Show ongoing partition movements",
		Long:  helpListMovement,
		Run: func(cmd *cobra.Command, topics []string) {
			f := p.Formatter
			if h, ok := f.Help(moveStatusResponse{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			// If partition(s) is specified but no topic(s) is specified, exit.
			if len(topics) <= 0 && len(partitions) > 0 {
				out.Die("specify at least one topic when --partition is used, exiting")
			}

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			response, err := cl.Reconfigurations(cmd.Context())
			out.MaybeDie(err, "unable to list partition movements: %v", err)

			if len(response) == 0 {
				if f.IsText() {
					out.Exit("There are no ongoing partition movements.")
				}
				printMoveStatus(f, buildMoveStatusResponse(nil, all), human, cmd.OutOrStdout())
				return
			}

			var filteredResponse []rpadmin.ReconfigurationsResponse
			for _, t := range topics {
				nt := strings.Split(t, "/")
				if len(nt) > 2 {
					fmt.Printf("invalid format for topic %s, skipping.\n", t)
					continue
				}
				for _, r := range response {
					isKafkaNs := len(nt) == 1 && r.Ns == "kafka" && r.Topic == t
					isInternalNs := len(nt) == 2 && r.Ns == nt[0] && r.Topic == nt[1]

					if isKafkaNs || isInternalNs {
						if len(partitions) == 0 || slices.Contains(partitions, strconv.Itoa(r.PartitionID)) {
							filteredResponse = append(filteredResponse, r)
						}
					}
				}
			}
			if len(filteredResponse) > 0 {
				response = filteredResponse
			}

			types.Sort(response)

			printMoveStatus(f, buildMoveStatusResponse(response, all), human, cmd.OutOrStdout())
		},
	}

	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print internal states about movements for debugging")
	cmd.Flags().BoolVarP(&human, "human-readable", "H", false, "Print the partition size in a human-readable form")
	cmd.Flags().StringSliceVarP(&partitions, "partition", "p", nil, "Partitions to filter ongoing movements status (repeatable)")
	p.InstallFormatFlag(cmd)

	return cmd
}

const helpListMovement = `Show ongoing partition movements.

By default this command lists all ongoing partition movements in the cluster.
Topics can be specified to print the move status of specific topics. By default,
this command assumes the "kafka" namespace, but you can use a "namespace/" to
specify internal namespaces.

    rpk cluster partitions move-status
    rpk cluster partitions move-status foo bar kafka_internal/tx

The "--partition / -p" flag can be used with topics to additional filter
requested partitions:

    rpk cluster partitions move-status foo bar --partition 0,1,2

The output contains the following columns with PARTITION-SIZE in bytes.
Using -H, it prints the partition size in a human-readable format

    NAMESPACE-TOPIC
    PARTITION
    MOVING-FROM
    MOVING-TO
    COMPLETION-%
    PARTITION-SIZE
    BYTES-MOVED
    BYTES-REMAINING

Using "--print-all / -a" the command additionally prints the column
"RECONCILIATION STATUSES", which reveals the internal status of the ongoing
reconciliations. Reported errors do not necessarily mean real problems.

The --format flag controls the output format: text (default), json, yaml, or
help (prints field descriptions).
`
