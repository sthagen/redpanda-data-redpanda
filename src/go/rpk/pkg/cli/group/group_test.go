// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintGroupList(t *testing.T) {
	cases := []struct {
		name string
		data []listedGroup
		want [][]string
	}{
		{
			name: "with state",
			data: []listedGroup{
				{Broker: 1, Group: "group-a", State: "Stable"},
				{Broker: 2, Group: "group-b", State: "Empty"},
			},
			want: [][]string{
				{"BROKER", "GROUP", "STATE"},
				{"1", "group-a", "Stable"},
				{"2", "group-b", "Empty"},
			},
		},
		{
			name: "without state",
			data: []listedGroup{
				{Broker: 1, Group: "group-a"},
				{Broker: 2, Group: "group-b"},
			},
			want: [][]string{
				{"BROKER", "GROUP"},
				{"1", "group-a"},
				{"2", "group-b"},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := config.OutFormatter{Kind: "text"}
			b := &strings.Builder{}
			printGroupList(f, c.data, b)
			require.Equal(t, c.want, out.TableRows(b.String()))
		})
	}
}

func TestPrintGroupDelete(t *testing.T) {
	data := []deletedGroup{
		{Group: "group-a", Status: "OK"},
		{Group: "group-b", Status: "some error"},
	}

	f := config.OutFormatter{Kind: "text"}
	b := &strings.Builder{}
	printGroupDelete(f, data, b)
	require.Equal(t, [][]string{
		{"GROUP", "STATUS"},
		{"group-a", "OK"},
		{"group-b", "some", "error"},
	}, out.TableRows(b.String()))
}
