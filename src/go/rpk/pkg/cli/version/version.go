// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package version

import (
	"fmt"
)

// Default build-time variables, passed via ldflags.
var (
	version   string // Semver of rpk.
	rev       string // Short git SHA.
	buildTime string // Timestamp of build time, RFC3339.
)

var (
	Version   = version
	Rev       = rev
	BuildTime = buildTime
)

func Pretty() string {
	return fmt.Sprintf("(Redpanda CLI): %s (rev %s)", version, rev)
}
