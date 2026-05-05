// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestPrintBrokers(t *testing.T) {
	rack := "rack-a"
	brokers := kadm.BrokerDetails{
		{NodeID: 0, Host: "10.0.0.1", Port: 9092, Rack: &rack},
		{NodeID: 1, Host: "10.0.0.2", Port: 9092, Rack: &rack},
	}

	t.Run("basic kafka metadata only", func(t *testing.T) {
		var buf bytes.Buffer
		printBrokers(&buf, 0, brokers, nil, false)
		require.Equal(t, [][]string{
			{"ID", "HOST", "PORT", "RACK"},
			{"0*", "10.0.0.1", "9092", "rack-a"},
			{"1", "10.0.0.2", "9092", "rack-a"},
		}, out.TableRows(buf.String()))
	})

	t.Run("detailed appends admin columns", func(t *testing.T) {
		alive := true
		info := &adminBrokerInfo{
			byNodeID: map[int32]rpadmin.Broker{
				0: {NodeID: 0, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
				1: {NodeID: 1, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
			},
		}
		var buf bytes.Buffer
		printBrokers(&buf, 0, brokers, info, false)
		require.Equal(t, [][]string{
			{"ID", "HOST", "PORT", "RACK", "CORES", "MEMBERSHIP", "IS-ALIVE", "VERSION"},
			{"0*", "10.0.0.1", "9092", "rack-a", "4", "active", "true", "24.3.1"},
			{"1", "10.0.0.2", "9092", "rack-a", "4", "active", "true", "24.3.1"},
		}, out.TableRows(buf.String()))
	})

	t.Run("uuid column appears when admin returns uuids", func(t *testing.T) {
		alive := true
		info := &adminBrokerInfo{
			byNodeID: map[int32]rpadmin.Broker{
				0: {NodeID: 0, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
				1: {NodeID: 1, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
			},
			uuidByNode: map[int32]string{
				0: "uuid-0",
				1: "uuid-1",
			},
		}
		var buf bytes.Buffer
		printBrokers(&buf, 0, brokers, info, false)
		rows := out.TableRows(buf.String())
		require.Equal(t, []string{"ID", "HOST", "PORT", "RACK", "CORES", "MEMBERSHIP", "IS-ALIVE", "VERSION", "UUID"}, rows[0])
		require.Equal(t, "uuid-0", rows[1][len(rows[1])-1])
	})

	t.Run("includeDecom appends decommissioned rows", func(t *testing.T) {
		alive := true
		info := &adminBrokerInfo{
			byNodeID: map[int32]rpadmin.Broker{
				0: {NodeID: 0, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
				1: {NodeID: 1, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
			},
			uuidByNode: map[int32]string{0: "uuid-0", 1: "uuid-1", 5: "uuid-5"},
			decom:      []rpadmin.BrokerUuids{{NodeID: 5, UUID: "uuid-5"}},
		}
		var buf bytes.Buffer
		printBrokers(&buf, 0, brokers, info, true)
		rows := out.TableRows(buf.String())
		// Decom row: NodeID + dashes + UUID at the end.
		decomRow := rows[len(rows)-1]
		require.Equal(t, "5", decomRow[0])
		require.Equal(t, "uuid-5", decomRow[len(decomRow)-1])
	})

	t.Run("detailed missing admin entry shows dashes", func(t *testing.T) {
		alive := true
		info := &adminBrokerInfo{
			byNodeID: map[int32]rpadmin.Broker{
				0: {NodeID: 0, NumCores: 4, MembershipStatus: rpadmin.MembershipStatusActive, IsAlive: &alive, Version: "v24.3.1"},
			},
		}
		var buf bytes.Buffer
		printBrokers(&buf, 0, brokers, info, false)
		rows := out.TableRows(buf.String())
		require.Equal(t, []string{"1", "10.0.0.2", "9092", "rack-a", "-", "-", "-", "-"}, rows[2])
	})
}

func TestPrintDiskSpace(t *testing.T) {
	info := &adminBrokerInfo{
		byNodeID: map[int32]rpadmin.Broker{
			0: {NodeID: 0, DiskSpace: []rpadmin.DiskSpace{
				{Path: "/data", Free: 50, Total: 100},
			}},
			1: {NodeID: 1, DiskSpace: []rpadmin.DiskSpace{
				{Path: "/data", Free: 25, Total: 100},
			}},
		},
	}
	var buf bytes.Buffer
	printDiskSpace(&buf, info)
	rows := out.TableRows(buf.String())
	require.Equal(t, []string{"NODE-ID", "PATH", "FREE", "TOTAL", "USED%"}, rows[0])
	require.Equal(t, "0", rows[1][0])
	require.Equal(t, "/data", rows[1][1])
	require.Equal(t, "50.0%", rows[1][len(rows[1])-1])
	require.Equal(t, "1", rows[2][0])
	require.Equal(t, "75.0%", rows[2][len(rows[2])-1])
}
