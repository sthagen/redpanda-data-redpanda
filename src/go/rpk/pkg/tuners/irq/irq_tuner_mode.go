// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package irq

/*
Modes are ordered from the one that cuts the biggest number of CPUs
from the compute CPUs' set to the one that takes the smallest ('mq' doesn't
cut any CPU from the compute set).
This fact is used when we calculate the 'common quotient' mode out of a
given set of modes (e.g. default modes of different Tuners) - this would
be the smallest among the given modes.

Modes description:
sq - set all IRQs of a given NIC to CPU0 and configure RPS

	to spreads NAPIs' handling between other CPUs.

sq_split - divide all IRQs of a given NIC between CPU0 and its HT siblings and configure RPS

	to spreads NAPIs' handling between other CPUs.

mq - distribute NIC's IRQs among all CPUs instead of binding

	them all to CPU0. In this mode RPS is always enabled to
	spreads NAPIs' handling between all CPUs.

dedicated:

	Use one interrupt core per 16 vcpus. Bind all NIC IRQs to these
	cores. Enable RPS/RFS to spread NAPI handling to all cores.

If there isn't any mode given script will use a default mode:
  - Use dedicated mode if it's allowed by config and safe to do so
  - Otherwise use mq mode
*/
type Mode string

const (
	SqSplit   Mode = "sq-split"
	Sq        Mode = "sq"
	Dedicated Mode = "dedicated"
	Mq        Mode = "mq"
	Default   Mode = "def"
)

func ModeFromString(modeString string) Mode {
	switch modeString {
	case "mq":
		return Mq
	case "sq":
		return Sq
	case "sq-split":
		return SqSplit
	case "dedicated":
		return Dedicated
	default:
		return Default
	}
}
