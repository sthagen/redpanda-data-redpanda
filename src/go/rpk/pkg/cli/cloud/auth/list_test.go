// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintCloudAuthList(t *testing.T) {
	data := []cloudAuthRow{
		{Name: "acme-sso", Kind: "sso", Organization: "acme", OrganizationID: "org-123", Current: true},
		{Name: "acme-client", Kind: "client", Organization: "acme", OrganizationID: "org-123"},
	}

	t.Run("text marks current with asterisk", func(t *testing.T) {
		var buf bytes.Buffer
		printCloudAuthList(config.OutFormatter{Kind: "text"}, data, &buf)
		require.Equal(t, [][]string{
			{"NAME", "KIND", "ORGANIZATION", "ORGANIZATION-ID"},
			{"acme-sso*", "sso", "acme", "org-123"},
			{"acme-client", "client", "acme", "org-123"},
		}, out.TableRows(buf.String()))
	})

	// Round-trip verifies that structured output uses the Current bool
	// and does not embed the asterisk in Name.
	t.Run("json round-trip", func(t *testing.T) {
		var buf bytes.Buffer
		printCloudAuthList(config.OutFormatter{Kind: "json"}, data, &buf)
		var got []cloudAuthRow
		require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
		require.Equal(t, data, got)
	})
}
