// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package upgrade

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

func newFinalizeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "finalize",
		Short: "Finalize a deferred cluster upgrade",
		Long: `Finalize a deferred cluster upgrade.

This commits the cluster to the version all brokers report, activating
version-gated features and advancing the active cluster version. It is
irreversible: the previous version is no longer available for downgrade.`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Validate upfront so we can fail fast with an actionable
			// message instead of submitting a request the controller
			// would silently drop.
			status, err := cl.FeaturesService().GetUpgradeStatus(cmd.Context(), connect.NewRequest(&adminv2.GetUpgradeStatusRequest{}))
			out.MaybeDie(err, "unable to retrieve upgrade status: %v", err)
			s := status.Msg

			if s.AutoFinalizationEnabled {
				out.Die("the cluster finalizes upgrades automatically (features_auto_finalization is enabled); set it to false to finalize manually")
			}
			switch s.State {
			case adminv2.FinalizationState_FINALIZATION_STATE_FINALIZED:
				out.Exit("Cluster is already finalized at logical version %d; nothing to do.", s.ActiveVersion)
			case adminv2.FinalizationState_FINALIZATION_STATE_UPGRADE_IN_PROGRESS:
				out.Die("cluster is not ready to finalize: not all brokers report the same version, or some are not live; run 'rpk cluster upgrade status' for per-broker details")
			case adminv2.FinalizationState_FINALIZATION_STATE_READY_TO_FINALIZE:
				// Proceed.
			default:
				out.Die("cluster is in an unexpected upgrade state %q; run 'rpk cluster upgrade status'", finalizationStateString(s.State))
			}

			if !noConfirm {
				confirmed, err := out.Confirm("Finalize the upgrade and advance the cluster to logical version %d? This is irreversible.", s.VersionAfterFinalization)
				out.MaybeDie(err, "unable to confirm finalization: %v", err)
				if !confirmed {
					out.Exit("Finalization canceled.")
				}
			}

			_, err = cl.FeaturesService().FinalizeUpgrade(cmd.Context(), connect.NewRequest(&adminv2.FinalizeUpgradeRequest{}))
			out.MaybeDie(err, "unable to finalize upgrade: %v", err)

			fmt.Printf("Upgrade finalization requested. Run 'rpk cluster upgrade status' to confirm the active version advanced to %d.\n", s.VersionAfterFinalization)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable the confirmation prompt")
	return cmd
}
