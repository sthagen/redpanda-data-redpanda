// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package network

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/stretchr/testify/require"
)

const PUCount = 8

type fakeMasks struct {
	irq.CPUMasks
	pus int
}

func intPtr(i int) *int    { return &i }
func boolPtr(b bool) *bool { return &b }

func (f *fakeMasks) GetAllCpusMask() (string, error)          { return "0xff", nil }
func (f *fakeMasks) GetNumberOfPUs(mask string) (uint, error) { return uint(f.pus), nil }

func TestCanDefaultToDedicatedMode(t *testing.T) {
	tests := []struct {
		name       string
		cliCpuset  string
		additional []string
		tuners     config.RpkNodeTuners
		want       bool
	}{
		{
			name:       "additional flags has cpuset",
			cliCpuset:  "0xff",
			additional: []string{"--cpuset", "0x1"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "tuner cli cpuset",
			cliCpuset:  "0x1",
			additional: []string{},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "smp unacceptable",
			cliCpuset:  "0xff",
			additional: []string{"--smp", "2"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "slightly lowered smp",
			cliCpuset:  "0xff",
			additional: []string{"--smp", "6"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(4), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       true,
		},
		{
			name:       "all checks pass",
			cliCpuset:  "0xff",
			additional: []string{},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &fakeMasks{}
			rnc := config.RpkNodeConfig{AdditionalStartFlags: tt.additional, Tuners: tt.tuners}
			got, err := canDefaultToDedicatedMode(PUCount, tt.cliCpuset, fm, rnc)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckDedicatedCompatibleConfig(t *testing.T) {
	tests := []struct {
		name       string
		cpuMask    string
		additional []string
		pus        int
		tuners     config.RpkNodeTuners
		wantErr    bool
	}{
		{
			name:       "additional flags has cpuset",
			cpuMask:    "0xff",
			additional: []string{"--cpuset", "0x1"},
			pus:        PUCount,
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount)},
			wantErr:    true,
		},
		{
			name:       "smp unacceptable",
			cpuMask:    "0xff",
			additional: []string{"--smp", "2"},
			pus:        PUCount,
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount)},
			wantErr:    true,
		},
		{
			name:       "tuner cpuset is acceptable",
			cpuMask:    "0xf",
			additional: []string{"--smp", "4"},
			pus:        4,
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(4)},
			wantErr:    false,
		},
		{
			name:       "smp acceptable",
			cpuMask:    "0xff",
			additional: []string{"--smp", "6"},
			pus:        PUCount,
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(4)},
			wantErr:    false,
		},
		{
			name:       "compatible config",
			cpuMask:    "0xff",
			additional: []string{},
			pus:        PUCount,
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount)},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &fakeMasks{pus: tt.pus}
			rnc := config.RpkNodeConfig{AdditionalStartFlags: tt.additional, Tuners: tt.tuners}
			err := checkDedicatedCompatibleConfig(tt.cpuMask, fm, rnc)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
