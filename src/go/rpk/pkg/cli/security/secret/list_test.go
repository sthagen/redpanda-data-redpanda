// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package secret

import (
	"bytes"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/stretchr/testify/require"
)

func TestPrintSecretList(t *testing.T) {
	f := config.OutFormatter{Kind: "text"}

	t.Run("multiple scopes", func(t *testing.T) {
		data := []secretListItem{
			{ID: "MY_SECRET", Scopes: []string{"redpanda_connect", "redpanda_cluster"}},
			{ID: "ANOTHER_SECRET", Scopes: []string{"redpanda_connect"}},
		}
		var buf bytes.Buffer
		printSecretList(f, data, &buf)
		require.Equal(t, [][]string{
			{"NAME", "SCOPES"},
			{"MY_SECRET", "redpanda_connect,", "redpanda_cluster"},
			{"ANOTHER_SECRET", "redpanda_connect"},
		}, out.TableRows(buf.String()))
	})

	t.Run("no scopes", func(t *testing.T) {
		var buf bytes.Buffer
		printSecretList(f, []secretListItem{{ID: "EMPTY_SECRET"}}, &buf)
		require.Equal(t, [][]string{
			{"NAME", "SCOPES"},
			{"EMPTY_SECRET"},
		}, out.TableRows(buf.String()))
	})

	t.Run("empty", func(t *testing.T) {
		var buf bytes.Buffer
		printSecretList(f, nil, &buf)
		require.Equal(t, [][]string{{"NAME", "SCOPES"}}, out.TableRows(buf.String()))
	})
}
