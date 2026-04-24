// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintAdminPartitionList(t *testing.T) {
	f := config.OutFormatter{Kind: "text"}

	t.Run("text", func(t *testing.T) {
		data := []partitionResponse{
			{Topic: "topic-a", Partition: 0, IsLeader: true},
			{Topic: "topic-a", Partition: 1, IsLeader: false},
			{Topic: "topic-b", Partition: 0, IsLeader: true},
		}
		var buf bytes.Buffer
		printAdminPartitionList(f, data, &buf)
		require.Equal(t, [][]string{
			{"TOPIC", "PARTITION", "IS-LEADER"},
			{"topic-a", "0", "true"},
			{"topic-a", "1", "false"},
			{"topic-b", "0", "true"},
		}, out.TableRows(buf.String()))
	})

	t.Run("text empty", func(t *testing.T) {
		var buf bytes.Buffer
		printAdminPartitionList(f, nil, &buf)
		require.Equal(t, [][]string{{"TOPIC", "PARTITION", "IS-LEADER"}}, out.TableRows(buf.String()))
	})
}
