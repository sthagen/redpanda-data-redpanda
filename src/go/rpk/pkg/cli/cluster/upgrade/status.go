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
	"strconv"
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// statusResponse is the formatted view of the FeaturesService.GetUpgradeStatus
// response.
type statusResponse struct {
	// State is the high-level finalization lifecycle state.
	State string `json:"state" yaml:"state"`
	// ActiveVersion is the active (committed) logical cluster version. It is
	// also the downgrade floor.
	ActiveVersion int64 `json:"active_version" yaml:"active_version"`
	// VersionAfterFinalization is the active version a finalize would produce
	// now. It is greater than ActiveVersion only when ready to finalize.
	VersionAfterFinalization int64 `json:"version_after_finalization" yaml:"version_after_finalization"`
	// AutoFinalizationEnabled reflects the features_auto_finalization cluster
	// config. When false, an explicit finalize is required.
	AutoFinalizationEnabled bool `json:"auto_finalization_enabled" yaml:"auto_finalization_enabled"`
	// Members is the per-broker reported version state.
	Members []statusMember `json:"members" yaml:"members"`
}

// statusMember is one broker's reported version state.
type statusMember struct {
	NodeID         int32  `json:"node_id" yaml:"node_id"`
	ReleaseVersion string `json:"release_version" yaml:"release_version"`
	LogicalVersion int64  `json:"logical_version" yaml:"logical_version"`
	VersionKnown   bool   `json:"version_known" yaml:"version_known"`
	Alive          bool   `json:"alive" yaml:"alive"`
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the cluster upgrade finalization status",
		Long: `Show the cluster upgrade finalization status.

This reports where the cluster sits in the upgrade-finalization lifecycle as
seen by the controller leader: the high-level state (finalized, ready to
finalize, or upgrade in progress), the active and post-finalization logical
versions, whether auto-finalization is enabled, and a per-broker breakdown of
reported versions and liveness.`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(statusResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			resp, err := cl.FeaturesService().GetUpgradeStatus(cmd.Context(), connect.NewRequest(&adminv2.GetUpgradeStatusRequest{}))
			out.MaybeDie(err, "unable to retrieve upgrade status: %v", err)

			err = printStatus(f, resp.Msg)
			out.MaybeDieErr(err)
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

// finalizationStateString renders an enum value as human-readable text by
// stripping the FINALIZATION_STATE_ prefix and lowercasing, e.g.
// FINALIZATION_STATE_READY_TO_FINALIZE -> "ready to finalize". Deriving it from
// the generated String() keeps it working when new states are added upstream.
func finalizationStateString(s adminv2.FinalizationState) string {
	name := strings.TrimPrefix(s.String(), "FINALIZATION_STATE_")
	return strings.ToLower(strings.ReplaceAll(name, "_", " "))
}

func buildStatusResponse(resp *adminv2.GetUpgradeStatusResponse) statusResponse {
	members := make([]statusMember, len(resp.Members))
	for i, m := range resp.Members {
		members[i] = statusMember{
			NodeID:         m.NodeId,
			ReleaseVersion: m.ReleaseVersion,
			LogicalVersion: m.LogicalVersion,
			VersionKnown:   m.VersionKnown,
			Alive:          m.Alive,
		}
	}
	return statusResponse{
		State:                    finalizationStateString(resp.State),
		ActiveVersion:            resp.ActiveVersion,
		VersionAfterFinalization: resp.VersionAfterFinalization,
		AutoFinalizationEnabled:  resp.AutoFinalizationEnabled,
		Members:                  members,
	}
}

func printStatus(f config.OutFormatter, msg *adminv2.GetUpgradeStatusResponse) error {
	resp := buildStatusResponse(msg)
	if isText, _, formatted, err := f.Format(resp); !isText {
		if err != nil {
			return fmt.Errorf("unable to print upgrade status in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(formatted)
		return nil
	}

	summary := out.NewTabWriter()
	summary.PrintColumn("State", resp.State)
	summary.PrintColumn("Active version", resp.ActiveVersion)
	summary.PrintColumn("Version after finalization", resp.VersionAfterFinalization)
	summary.PrintColumn("Auto-finalization enabled", resp.AutoFinalizationEnabled)
	summary.Flush()

	if len(resp.Members) > 0 {
		fmt.Println()
		tw := out.NewTable("NODE", "RELEASE-VERSION", "LOGICAL-VERSION", "VERSION-KNOWN", "ALIVE")
		defer tw.Flush()
		for _, m := range resp.Members {
			logicalVersion := strconv.FormatInt(m.LogicalVersion, 10)
			if !m.VersionKnown {
				logicalVersion = "-"
			}
			releaseVersion := m.ReleaseVersion
			if releaseVersion == "" {
				releaseVersion = "-"
			}
			tw.Print(m.NodeID, releaseVersion, logicalVersion, m.VersionKnown, m.Alive)
		}
	}
	return nil
}
