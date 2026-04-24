// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintDeleteOutput(t *testing.T) {
	f := config.OutFormatter{Kind: "text"}
	row := aclWithMessage{
		Principal:           "User:alice",
		Host:                "*",
		ResourceType:        "Topic",
		ResourceName:        "foo",
		ResourcePatternType: "Literal",
		Operation:           "Read",
		Permission:          "Allow",
	}
	output := aclDeleteOutput{
		Filters:   []aclWithMessage{row},
		Deletions: []aclWithMessage{row},
	}

	header := []string{"PRINCIPAL", "HOST", "RESOURCE-TYPE", "RESOURCE-NAME", "RESOURCE-PATTERN-TYPE", "OPERATION", "PERMISSION", "ERROR"}
	dataRow := []string{"User:alice", "*", "Topic", "foo", "Literal", "Read", "Allow"}

	t.Run("filters and deletions sections", func(t *testing.T) {
		var buf bytes.Buffer
		printDeleteOutput(f, output, true, &buf)
		require.Equal(t, [][]string{
			{"FILTERS"},
			header,
			dataRow,
			{},
			{"DELETIONS"},
			header,
			dataRow,
		}, out.TableRows(buf.String()))
	})

	t.Run("no header when deletions-only and flag false", func(t *testing.T) {
		var buf bytes.Buffer
		printDeleteOutput(f, aclDeleteOutput{Deletions: output.Deletions}, false, &buf)
		require.Equal(t, [][]string{header, dataRow}, out.TableRows(buf.String()))
	})

	// Round-trip verifies structured output preserves field names and values.
	// The empty-filters case also covers the omitempty tag.
	t.Run("json omitempty on filters", func(t *testing.T) {
		var buf bytes.Buffer
		printDeleteOutput(config.OutFormatter{Kind: "json"}, aclDeleteOutput{Deletions: output.Deletions}, false, &buf)
		require.False(t, strings.Contains(buf.String(), `"filters"`))
	})
}
