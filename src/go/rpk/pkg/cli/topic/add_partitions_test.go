// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintAddPartitionsResults(t *testing.T) {
	results := []addPartitionsResult{
		{Topic: "foo", Status: "OK"},
		{Topic: "bar", Status: "INVALID_PARTITIONS: unable to add 3 partitions due to hardware constraints"},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printAddPartitionsResults(f, results, b)
	require.Equal(t, [][]string{
		{"TOPIC", "STATUS"},
		{"foo", "OK"},
		{"bar", "INVALID_PARTITIONS:", "unable", "to", "add", "3", "partitions", "due", "to", "hardware", "constraints"},
	}, out.TableRows(b.String()))
}
