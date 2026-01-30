// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseMetadataProperties(t *testing.T) {
	for _, tt := range []struct {
		name   string
		input  []string
		exp    map[string]string
		expErr bool
	}{
		{name: "empty input", input: nil, exp: nil},
		{name: "single key=value", input: []string{"key=value"}, exp: map[string]string{"key": "value"}},
		{name: "multiple key=value", input: []string{"k1=v1", "k2=v2"}, exp: map[string]string{"k1": "v1", "k2": "v2"}},
		{name: "key=value with spaces", input: []string{" key = value "}, exp: map[string]string{"key": "value"}},
		{name: "value with equals", input: []string{"key=val=ue"}, exp: map[string]string{"key": "val=ue"}},
		{name: "json object", input: []string{`{"k1":"v1","k2":"v2"}`}, exp: map[string]string{"k1": "v1", "k2": "v2"}},
		{name: "json with spaces", input: []string{` {"k1":"v1"} `}, exp: map[string]string{"k1": "v1"}},
		{name: "json with special chars in value", input: []string{`{"key":"a=b,c:d"}`}, exp: map[string]string{"key": "a=b,c:d"}},
		{name: "mixed json and kv", input: []string{`{"k1":"v1"}`, "k2=v2"}, exp: map[string]string{"k1": "v1", "k2": "v2"}},
		{name: "key starting with brace", input: []string{"{mykey=value"}, exp: map[string]string{"{mykey": "value"}},
		{name: "braces with kv fallback", input: []string{"{a=b}"}, exp: map[string]string{"{a": "b}"}},
		{name: "no delimiter fails", input: []string{"nodeli"}, expErr: true},
		{name: "empty key fails", input: []string{"=value"}, expErr: true},
		{name: "empty value fails", input: []string{"key="}, expErr: true},
		{name: "invalid json falls back to kv and fails", input: []string{`{invalid}`}, expErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMetadataProperties(tt.input)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}
