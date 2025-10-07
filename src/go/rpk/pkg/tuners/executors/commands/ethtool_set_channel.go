// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package commands

import (
	"bufio"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	et "github.com/safchain/ethtool"
	"go.uber.org/zap"
)

type ethtoolSetChannelCommand struct {
	Command
	intf     string
	channels et.Channels
	ethtool  ethtool.EthtoolWrapper
}

func NewEthtoolSetChannelCmd(
	ethtool ethtool.EthtoolWrapper, intf string, channels et.Channels,
) Command {
	return &ethtoolSetChannelCommand{
		intf:     intf,
		channels: channels,
		ethtool:  ethtool,
	}
}

func (c *ethtoolSetChannelCommand) Execute() error {
	zap.L().Sugar().Debugf("Changing interface '%s', channel counts '%v'", c.intf, c.channels)
	_, err := c.ethtool.SetChannels(c.intf, c.channels)
	return err
}

func (c *ethtoolSetChannelCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "ethtool -L %s combined %d rx %d tx %d other %d\n",
		c.intf, c.channels.CombinedCount, c.channels.RxCount, c.channels.TxCount, c.channels.OtherCount)
	return w.Flush()
}
