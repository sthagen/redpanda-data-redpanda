// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintProfileList(t *testing.T) {
	profiles := []profileListItem{
		{Name: "dev", Description: "development cluster", Current: true},
		{Name: "prod", Description: ""},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printProfileList(f, profiles, b)
	require.Equal(t, [][]string{
		{"NAME", "DESCRIPTION"},
		{"dev*", "development", "cluster"},
		{"prod"},
	}, out.TableRows(b.String()))
}
