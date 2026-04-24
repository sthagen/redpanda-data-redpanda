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

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/twmb/types"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type balancerStatusResponse struct {
	Status                          string                `json:"status" yaml:"status"`
	SecondsSinceLastTick            int                   `json:"seconds_since_last_tick" yaml:"seconds_since_last_tick"`
	CurrentReassignmentsCount       int                   `json:"current_reassignments_count" yaml:"current_reassignments_count"`
	PartitionsPendingForceRecovery  *int                  `json:"partitions_pending_force_recovery_count,omitempty" yaml:"partitions_pending_force_recovery_count,omitempty"`
	PartitionsPendingRecoverySample []string              `json:"partitions_pending_force_recovery_sample,omitempty" yaml:"partitions_pending_force_recovery_sample,omitempty"`
	UnavailableNodes                []int                 `json:"unavailable_nodes,omitempty" yaml:"unavailable_nodes,omitempty"`
	OverDiskLimitNodes              []int                 `json:"over_disk_limit_nodes,omitempty" yaml:"over_disk_limit_nodes,omitempty"`
	BrokerReplicaDistribution       []ReplicaDistribution `json:"broker_replica_distribution,omitempty" yaml:"broker_replica_distribution,omitempty"`
}

func buildBalancerStatusResponse(pbs rpadmin.PartitionBalancerStatus, clusterPartitions []rpadmin.ClusterPartition) balancerStatusResponse {
	replicaDist := buildReplicaPerBroker(clusterPartitions)
	types.Sort(replicaDist)

	resp := balancerStatusResponse{
		Status:                          pbs.Status,
		SecondsSinceLastTick:            pbs.SecondsSinceLastTick,
		CurrentReassignmentsCount:       pbs.CurrentReassignmentsCount,
		PartitionsPendingForceRecovery:  pbs.PartitionsPendingForceRecovery,
		PartitionsPendingRecoverySample: pbs.PartitionsPendingRecoveryList,
		UnavailableNodes:                pbs.Violations.UnavailableNodes,
		OverDiskLimitNodes:              pbs.Violations.OverDiskLimitNodes,
		BrokerReplicaDistribution:       replicaDist,
	}
	return resp
}

func printBalancerStatus(f config.OutFormatter, resp balancerStatusResponse, w io.Writer) {
	if isText, _, t, err := f.Format(resp); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}

	const (
		secBalancerStatus = "Balancer status"
		secReplicaDist    = "Replica distribution"
	)
	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secBalancerStatus: true,
			secReplicaDist:    len(resp.BrokerReplicaDistribution) > 0,
		})...,
	)
	sections.Add(secBalancerStatus, func() {
		tw := out.NewTableTo(w)
		defer tw.Flush()
		tw.Print("Status:", resp.Status)
		tw.Print("Seconds Since Last Tick:", resp.SecondsSinceLastTick)
		tw.Print("Current Reassignment Count:", resp.CurrentReassignmentsCount)
		if resp.PartitionsPendingForceRecovery != nil {
			tw.Print(fmt.Sprintf("Partitions Pending Recovery (%v):", *resp.PartitionsPendingForceRecovery), resp.PartitionsPendingRecoverySample)
		}
		if len(resp.UnavailableNodes) > 0 || len(resp.OverDiskLimitNodes) > 0 {
			tw.Print("Unavailable Nodes:", resp.UnavailableNodes)
			tw.Print("Over Disk Limit Nodes:", resp.OverDiskLimitNodes)
		}
	})
	sections.Add(secReplicaDist, func() {
		tw := out.NewTableTo(w, "BROKER", "PARTITION-COUNT")
		defer tw.Flush()
		for _, d := range resp.BrokerReplicaDistribution {
			tw.Print(d.NodeID, d.Count)
		}
	})
}

func newBalancerStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "balancer-status",
		Short: "Queries cluster for partition balancer status",
		Long: `Queries cluster for partition balancer status:

If continuous partition balancing is enabled, redpanda will continuously
reassign partitions from both unavailable nodes and from nodes using more disk
space than the configured limit.

Use this command to monitor the partition balancer status and
the partition distribution across brokers in the cluster.

FIELDS

    Status:                        Either off, ready, starting, in progress, or
                                   stalled.
    Seconds Since Last Tick:       The last time the partition balancer ran.
    Current Reassignments Count:   Current number of partition reassignments in
                                   progress.
    Unavailable Nodes:             The nodes that have been unavailable after a
                                   time set by the
                                   "partition_autobalancing_node_availability_timeout_sec"
                                   cluster property.
    Over Disk Limit Nodes:         The nodes that surpassed the threshold of
                                   used disk percentage specified in the
                                   "partition_autobalancing_max_disk_usage_percent"
                                   cluster property.

BALANCER STATUS

    off:          The balancer is disabled.
    ready:        The balancer is active but there is nothing to do.
    starting:     The balancer is starting but has not run yet.
    in_progress:  The balancer is active and is in the process of scheduling
                  partition movements.
    stalled:      Violations have been detected and the balancer cannot correct
                  them.

STALLED BALANCER

A stalled balancer can occur for a few reasons and requires a bit of manual
investigation. A few areas to investigate:

* Are there are enough healthy nodes to which to move partitions? For example,
  in a three node cluster, no movements are possible for partitions with three
  replicas. You will see a stall every time there is a violation.

* Does the cluster have sufficient space? If all nodes in the cluster are
  utilizing more than 80% of their disk space, rebalancing cannot proceed.

* Do all partitions have quorum? If the majority of a partition's replicas are
  down, the partition cannot be moved.

* Are any nodes in maintenance mode? Partitions are not moved if any node is in
  maintenance mode.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(balancerStatusResponse{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			status, err := cl.GetPartitionStatus(cmd.Context())
			out.MaybeDie(err, "unable to request balancer status: %v", err)

			var clusterPartitions []rpadmin.ClusterPartition
			clusterPartitions, err = cl.AllClusterPartitions(cmd.Context(), true, false)
			if err != nil {
				fmt.Printf("unable to query all partitions in the cluster: %v", err)
			}

			printBalancerStatus(f, buildBalancerStatusResponse(status, clusterPartitions), cmd.OutOrStdout())
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
