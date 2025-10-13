// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tuners

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

func preferredClockSource() string {
	switch runtime.GOARCH {
	case "amd64", "386":
		return "tsc"
	case "arm64":
		return "arch_sys_counter"
	default:
		return ""
	}
}

func NewClockSourceChecker(fs afero.Fs) Checker {
	pref := preferredClockSource()
	return NewEqualityChecker(
		ClockSource,
		"Clock Source",
		Warning,
		pref,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs,
				"/sys/devices/system/clocksource/clocksource0/current_clocksource")
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(string(content)), nil
		},
	)
}

func NewClockSourceTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewClockSourceChecker(fs),
		func() TuneResult {
			pref := preferredClockSource()
			if pref == "" {
				return NewTuneError(fmt.Errorf("clocksource setting not available for this architecture"))
			}
			err := executor.Execute(commands.NewWriteFileCmd(fs,
				"/sys/devices/system/clocksource/clocksource0/current_clocksource",
				pref))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			pref := preferredClockSource()
			if pref == "" {
				return false, "Clocksource setting not available for this architecture"
			}

			content, err := afero.ReadFile(fs,
				"/sys/devices/system/clocksource/clocksource0/available_clocksource")
			if err != nil {
				return false, err.Error()
			}
			availableSrcs := strings.Fields(string(content))

			for _, src := range availableSrcs {
				if src == pref {
					return true, ""
				}
			}
			return false, fmt.Sprintf(
				"Preferred clocksource '%s' not available", pref)
		},
		executor.IsLazy(),
	)
}
