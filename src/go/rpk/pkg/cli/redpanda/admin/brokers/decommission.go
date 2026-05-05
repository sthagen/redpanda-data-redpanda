// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/brokers"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDecommissionBroker(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(brokers.NewDecommissionBroker(fs, p), "rpk cluster brokers decommission")
}
