// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintPluginList(t *testing.T) {
	data := []pluginRow{
		{Name: "cloud", Description: "Manage Redpanda Cloud", Installed: true},
		{Name: "byoc", Description: "Bring your own cloud", Installed: false},
		{Name: "mm3", Description: "MirrorMaker3", Installed: true, Message: "sha differs"},
	}

	var buf bytes.Buffer
	printPluginList(config.OutFormatter{Kind: "text"}, data, &buf)
	require.Equal(t, [][]string{
		{"NAME", "DESCRIPTION", "MESSAGE"},
		{"*cloud", "Manage", "Redpanda", "Cloud"},
		{"byoc", "Bring", "your", "own", "cloud"},
		{"*mm3", "MirrorMaker3", "sha", "differs"},
	}, out.TableRows(buf.String()))
}

func TestPrintLocalPluginList(t *testing.T) {
	data := []localPluginRow{
		{Name: "cloud", Path: "/home/user/.local/bin/.rpk-cloud"},
		{Name: "byoc", Path: "/home/user/.local/bin/.rpk-byoc", Shadows: []string{"/usr/local/bin/.rpk-byoc", "/opt/bin/.rpk-byoc"}},
	}

	var buf bytes.Buffer
	printLocalPluginList(config.OutFormatter{Kind: "text"}, data, &buf)
	// Second shadow appears on its own row with blank Name/Path cells.
	require.Equal(t, [][]string{
		{"NAME", "PATH", "SHADOWS"},
		{"cloud", "/home/user/.local/bin/.rpk-cloud"},
		{"byoc", "/home/user/.local/bin/.rpk-byoc", "/usr/local/bin/.rpk-byoc"},
		{"/opt/bin/.rpk-byoc"},
	}, out.TableRows(buf.String()))
}
