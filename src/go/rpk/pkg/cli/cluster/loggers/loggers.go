// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package loggers contains commands to list and set Redpanda broker
// log levels via the Admin API.
package loggers

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCommand returns the cluster loggers command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "loggers",
		Short: "List and configure Redpanda broker loggers",
		Args:  cobra.ExactArgs(0),
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		NewListCommand(fs, p),
		NewSetCommand(fs, p),
	)
	return cmd
}

// parseHelpLoggersOutput parses the output of 'redpanda --help-loggers'.
// It looks for the "Available loggers:" header, then collects all subsequent
// indented lines as logger names.
func parseHelpLoggersOutput(output string) ([]string, error) {
	const header = "Available loggers:"
	lines := strings.Split(output, "\n")
	headerIdx := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == header {
			headerIdx = i
			break
		}
	}
	if headerIdx == -1 {
		return nil, fmt.Errorf("header %q not found in output", header)
	}
	// Collect indented lines after the header
	var loggers []string
	for _, line := range lines[headerIdx+1:] {
		if line == "" {
			continue
		}
		// Stop at first non-indented line
		if len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			break
		}
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			loggers = append(loggers, trimmed)
		}
	}
	if len(loggers) == 0 {
		return nil, fmt.Errorf("no loggers found in output")
	}
	sort.Strings(loggers)
	return loggers, nil
}

// loggersFromBinary attempts to discover available loggers by running the
// local redpanda binary with --help-loggers. This only works on Linux.
func loggersFromBinary(ctx context.Context, fs afero.Fs) ([]string, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("binary discovery only supported on linux")
	}
	installDir, err := redpanda.FindInstallDir(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to find redpanda install directory: %v", err)
	}
	bin := filepath.Join(installDir, "bin", "redpanda")
	output, err := exec.CommandContext(ctx, bin, "--help-loggers").Output()
	if err != nil {
		return nil, fmt.Errorf("unable to run %s --help-loggers: %v", bin, err)
	}
	return parseHelpLoggersOutput(string(output))
}

// DiscoverLoggers returns the list of available loggers using a fallback chain:
// local binary -> Admin API -> hardcoded default list.
func DiscoverLoggers(ctx context.Context, cl *rpadmin.AdminAPI, fs afero.Fs) []string {
	// Attempt 1: Local binary (Linux only).
	loggers, err := loggersFromBinary(ctx, fs)
	if err == nil && len(loggers) > 0 {
		return loggers
	}
	zap.L().Sugar().Debugf("Unable to discover loggers from local binary: %v; trying Admin API", err)

	// Attempt 2: Admin API.
	levels, err := cl.GetLogLevels(ctx)
	if err == nil && len(levels) > 0 {
		names := make([]string, 0, len(levels))
		for _, l := range levels {
			if l.Name != "" {
				names = append(names, l.Name)
			}
		}
		sort.Strings(names)
		return names
	}
	zap.L().Sugar().Debugf("Unable to discover loggers from Admin API: %v; using default list", err)

	// Attempt 3: Hardcoded fallback.
	return defaultLoggers
}

// defaultLoggers is the fallback list of loggers used when dynamic discovery
// fails. To regenerate this list, run 'redpanda --help-loggers'.
var defaultLoggers = []string{
	"abs",
	"admin/proxy/client",
	"admin/proxy/service",
	"admin_api_server",
	"admin_api_server/broker_service",
	"admin_api_server/cluster_service",
	"admin_api_server/internal_breakglass_service",
	"admin_api_server/internal_debug_service",
	"admin_api_server/security_service",
	"archival",
	"archival-ctrl",
	"assert",
	"auditing",
	"client_config",
	"client_pool",
	"cloud_io",
	"cloud_roles",
	"cloud_storage",
	"cloud_topics",
	"cloud_topics_compaction",
	"cluster",
	"compaction_ctrl",
	"compression",
	"config",
	"connectrpc",
	"controller_rate_limiter_log",
	"cpu_profiler",
	"crash-reporter",
	"crash_tracker",
	"data-migrate",
	"datalake",
	"debug-bundle-service",
	"dl_backlog_controller",
	"dns_resolver",
	"exception",
	"fault_injector",
	"features",
	"finject",
	"http",
	"httpd",
	"iceberg",
	"io",
	"json",
	"kafka",
	"kafka-cg",
	"kafka/authz",
	"kafka/client",
	"kafka/data/rpc",
	"kafka_data",
	"kafka_quotas",
	"kvstore",
	"level_zero_gc_service",
	"lsm",
	"main",
	"metrics-reporter",
	"net_tls",
	"offset_translator",
	"ossl-library-context-service",
	"pandaproxy",
	"pandaproxy/requests",
	"r/heartbeat",
	"raft",
	"reconciler",
	"request_auth",
	"resource_mgmt",
	"resources",
	"rpc",
	"s3",
	"schemaregistry",
	"schemaregistry/requests",
	"scollectd",
	"seastar",
	"seastar-tls",
	"seastar_memory",
	"security",
	"serde",
	"shadow_link",
	"shadow_link_internal_service",
	"shadow_link_service",
	"storage",
	"storage-gc",
	"storage-resources",
	"syschecks",
	"transform",
	"transform/logging",
	"transform/rpc",
	"transform/stm",
	"tx",
	"tx-migration",
	"wasm",
}
