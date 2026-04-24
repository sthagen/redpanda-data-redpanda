// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"strings"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestBuildNodeStatuses(t *testing.T) {
	nodes := rpadmin.ConfigStatusResponse{
		{NodeID: 1, ConfigVersion: 5, Restart: true, Invalid: []string{"bad_key"}, Unknown: []string{"new_key"}},
		{NodeID: 2, ConfigVersion: 5, Restart: false, Invalid: nil, Unknown: nil},
	}

	statuses := buildNodeStatuses(nodes)
	require.Len(t, statuses, 2)

	require.Equal(t, int64(1), statuses[0].Node)
	require.Equal(t, int64(5), statuses[0].ConfigVersion)
	require.True(t, statuses[0].NeedsRestart)
	require.Equal(t, []string{"bad_key"}, statuses[0].Invalid)
	require.Equal(t, []string{"new_key"}, statuses[0].Unknown)

	require.Equal(t, int64(2), statuses[1].Node)
	require.False(t, statuses[1].NeedsRestart)
	require.Nil(t, statuses[1].Invalid)
	require.Nil(t, statuses[1].Unknown)
}

func TestPrintNodeStatus(t *testing.T) {
	statuses := []nodeConfigStatus{
		{Node: 1, ConfigVersion: 5, NeedsRestart: true, Invalid: []string{"bad_key"}, Unknown: []string{"new_key"}},
		{Node: 2, ConfigVersion: 5, NeedsRestart: false},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printNodeStatus(f, statuses, b)
	require.Equal(t, [][]string{
		{"NODE", "CONFIG-VERSION", "NEEDS-RESTART", "INVALID", "UNKNOWN"},
		{"1", "5", "true", "[bad_key]", "[new_key]"},
		{"2", "5", "false", "[]", "[]"},
	}, out.TableRows(b.String()))
}
