// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToScreamingSnakeCase(t *testing.T) {
	tests := []struct {
		name  string
		input string
		exp   string
	}{
		{"simple two words", "PatternType", "PATTERN_TYPE"},
		{"acronym followed by word", "ACLResource", "ACL_RESOURCE"},
		{"acronym followed by multiple words", "ACLPermissionType", "ACL_PERMISSION_TYPE"},
		{"three words", "ShadowLinkState", "SHADOW_LINK_STATE"},
		{"three words with acronym", "ShadowACLState", "SHADOW_ACL_STATE"},
		{"acronym only", "ACL", "ACL"},
		{"single word", "State", "STATE"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toScreamingSnakeCase(tt.input)
			require.Equal(t, tt.exp, got)
		})
	}
}
