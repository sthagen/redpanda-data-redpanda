// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package network

const (
	RfsTableSizeProperty = "net.core.rps_sock_flow_entries"
	ListenBacklogFile    = "/proc/sys/net/core/somaxconn"
	SynBacklogFile       = "/proc/sys/net/ipv4/tcp_max_syn_backlog"
	RfsTableSize         = 32768
	SynBacklogSize       = 4096
	ListenBacklogSize    = 4096
	MaxInt               = int(^uint(0) >> 1)
	// We store the config in /var/run such that it survives till reboot (but only until then - tmpfs).
	// /var/run requires root but the tuner requires that anyway.
	// /tmp is not suitable because of things like systemd-tmpfiles that can clear it out.
	DefaultNodeTunerStateFile = "/var/run/redpanda_node_tuner_state.yaml"
)
