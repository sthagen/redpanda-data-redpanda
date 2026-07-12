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
	"testing"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"github.com/stretchr/testify/require"
)

func TestBuildStatusResponse(t *testing.T) {
	msg := &adminv2.GetUpgradeStatusResponse{
		State:                    adminv2.FinalizationState_FINALIZATION_STATE_READY_TO_FINALIZE,
		ActiveVersion:            12,
		VersionAfterFinalization: 13,
		AutoFinalizationEnabled:  false,
		Members: []*adminv2.MemberVersion{
			{NodeId: 0, LogicalVersion: 13, VersionKnown: true, Alive: true, ReleaseVersion: "v25.2.1"},
			{NodeId: 1, LogicalVersion: 0, VersionKnown: false, Alive: false, ReleaseVersion: ""},
		},
	}
	exp := statusResponse{
		State:                    "ready to finalize",
		ActiveVersion:            12,
		VersionAfterFinalization: 13,
		AutoFinalizationEnabled:  false,
		Members: []statusMember{
			{NodeID: 0, LogicalVersion: 13, VersionKnown: true, Alive: true, ReleaseVersion: "v25.2.1"},
			{NodeID: 1, LogicalVersion: 0, VersionKnown: false, Alive: false, ReleaseVersion: ""},
		},
	}
	require.Equal(t, exp, buildStatusResponse(msg))
}
