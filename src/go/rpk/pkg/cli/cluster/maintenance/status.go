// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package maintenance

import (
	"fmt"
	"io"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type brokerMaintenanceStatus struct {
	NodeID       int   `json:"node_id" yaml:"node_id"`
	Enabled      bool  `json:"enabled" yaml:"enabled"`
	Finished     *bool `json:"finished,omitempty" yaml:"finished,omitempty"`
	Errors       *bool `json:"errors,omitempty" yaml:"errors,omitempty"`
	Partitions   *int  `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	Eligible     *int  `json:"eligible,omitempty" yaml:"eligible,omitempty"`
	Transferring *int  `json:"transferring,omitempty" yaml:"transferring,omitempty"`
	Failed       *int  `json:"failed,omitempty" yaml:"failed,omitempty"`
}

func buildMaintenanceStatuses(brokers []rpadmin.Broker) []brokerMaintenanceStatus {
	statuses := make([]brokerMaintenanceStatus, 0, len(brokers))
	for _, b := range brokers {
		s := brokerMaintenanceStatus{NodeID: b.NodeID}
		if b.Maintenance != nil {
			s.Enabled = b.Maintenance.Draining
			s.Finished = b.Maintenance.Finished
			s.Errors = b.Maintenance.Errors
			s.Partitions = b.Maintenance.Partitions
			s.Eligible = b.Maintenance.Eligible
			s.Transferring = b.Maintenance.Transferring
			s.Failed = b.Maintenance.Failed
		}
		statuses = append(statuses, s)
	}
	return statuses
}

func printMaintenanceStatus(f config.OutFormatter, statuses []brokerMaintenanceStatus, w io.Writer) {
	if isText, _, t, err := f.Format(statuses); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := newMaintenanceReportTable(w)
	defer tw.Flush()
	for _, s := range statuses {
		tw.Print(
			s.NodeID,
			s.Enabled,
			nullableToStr(s.Finished),
			nullableToStr(s.Errors),
			nullableToStr(s.Partitions),
			nullableToStr(s.Eligible),
			nullableToStr(s.Transferring),
			nullableToStr(s.Failed),
		)
	}
}

func nullableToStr[V any](v *V) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprint(*v)
}

func newMaintenanceReportTable(w io.Writer) *out.TabWriter {
	return out.NewTableTo(w, "Node-ID", "Enabled", "Finished", "Errors", "Partitions", "Eligible", "Transferring", "Failed")
}

func addBrokerMaintenanceReport(table *out.TabWriter, b rpadmin.Broker) {
	table.Print(
		b.NodeID,
		b.Maintenance.Draining,
		nullableToStr(b.Maintenance.Finished),
		nullableToStr(b.Maintenance.Errors),
		nullableToStr(b.Maintenance.Partitions),
		nullableToStr(b.Maintenance.Eligible),
		nullableToStr(b.Maintenance.Transferring),
		nullableToStr(b.Maintenance.Failed))
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report maintenance status",
		Long: `Report maintenance status.

This command reports maintenance status for each node in the cluster. The output
is presented as a table with each row representing a node in the cluster.  The
output can be used to monitor the progress of node draining.

   NODE-ID  ENABLED  FINISHED  ERRORS  PARTITIONS  ELIGIBLE  TRANSFERRING  FAILED
   1        false     false     false   0           0         0             0

Field descriptions:

        NODE-ID: the node ID
        ENABLED: true if the node is currently in maintenance mode (draining)
       FINISHED: leadership draining has completed
         ERRORS: errors have been encountered while draining
     PARTITIONS: number of partitions whose leadership has moved
       ELIGIBLE: number of partitions with leadership eligible to move
   TRANSFERRING: current active number of leadership transfers
         FAILED: number of failed leadership transfers

Notes:

   - When errors are present further information will be available in the logs
     for the corresponding node.

   - Only partitions with more than one replica are eligible for leadership
     transfer.

   - FINISHED, ERRORS, PARTITIONS, ELIGIBLE, TRANSFERRING, and FAILED are only
     populated while a node is in maintenance mode (ENABLED=true).
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]brokerMaintenanceStatus{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			brokers, err := client.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to request brokers: %v", err)

			if len(brokers) == 0 {
				out.Die("no brokers found; check broker address configuration")
			}

			if brokers[0].Maintenance == nil {
				out.Die("maintenance mode is not supported in this cluster")
			}

			printMaintenanceStatus(f, buildMaintenanceStatuses(brokers), cmd.OutOrStdout())
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
