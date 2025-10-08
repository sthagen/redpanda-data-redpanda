// Copyright 2020 Redpanda Data, Inc.
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
	"fmt"
	"math"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type procFileMock struct {
	irq.ProcFile
	getIRQProcFileLinesMap func() (map[int]string, error)
}

func (m *procFileMock) GetIRQProcFileLinesMap() (map[int]string, error) {
	return m.getIRQProcFileLinesMap()
}

type deviceInfoMock struct {
	irq.DeviceInfo
	getIRQs func(string, string) ([]int, error)
}

func (m *deviceInfoMock) GetIRQs(path string, device string) ([]int, error) {
	return m.getIRQs(path, device)
}

type ethtoolMock struct {
	ethtool.EthtoolWrapper
	driverName func(string) (string, error)
	features   func(string) (map[string]bool, error)
}

func (m *ethtoolMock) DriverName(iface string) (string, error) {
	if m.driverName == nil {
		return "dummy", nil
	}
	return m.driverName(iface)
}

func (m *ethtoolMock) Features(iface string) (map[string]bool, error) {
	return m.features(iface)
}

func Test_nic_IsBondIface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0o644)
	// when
	bond := nic.IsBondIface()
	// then
	require.True(t, bond)
}

func Test_nic_Slaves_ReturnAllSlavesOfAnInterface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0o644)
	afero.WriteFile(fs, "/sys/class/net/test0/bond/slaves", []byte("sl0\nsl1\nsl2"), 0o644)
	// when
	slaves, err := nic.Slaves()
	// then
	require.NoError(t, err)
	require.Len(t, slaves, 3)
	require.Equal(t, slaves[0].Name(), "sl0")
	require.Equal(t, slaves[1].Name(), "sl1")
	require.Equal(t, slaves[2].Name(), "sl2")
}

func Test_nic_Slaves_ReturnEmptyForNotBondInterface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	// when
	slaves, err := nic.Slaves()
	// then
	require.NoError(t, err)
	require.Empty(t, slaves)
}

type IrqInfoRes struct {
	Num        int
	ProcLine   string
	QueueIndex int
}

func Test_nic_GetIRQs(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		nicName       string
		want          []IrqInfoRes
		driverName    string
	}{
		{
			name: "Shall return all device IRQs when there are not fast paths",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						54: "54:       9076       8545       3081       1372       4662     190816       3865       6709  IR-PCI-MSI 333825-edge      iwlwifi: queue 1",
						56: "56:      24300       3370        681       2725       1511       6627      21983       7056  IR-PCI-MSI 333826-edge      iwlwifi: queue 2",
						58: "58:       8444      10072       3025       2732       5432       5919       7217       3559  IR-PCI-MSI 333827-edge      iwlwifi: queue 3",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{54, 56, 58}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        54,
					ProcLine:   "54:       9076       8545       3081       1372       4662     190816       3865       6709  IR-PCI-MSI 333825-edge      iwlwifi: queue 1",
					QueueIndex: math.MaxInt64,
				},
				{
					Num:        56,
					ProcLine:   "56:      24300       3370        681       2725       1511       6627      21983       7056  IR-PCI-MSI 333826-edge      iwlwifi: queue 2",
					QueueIndex: math.MaxInt64,
				},
				{
					Num:        58,
					ProcLine:   "58:       8444      10072       3025       2732       5432       5919       7217       3559  IR-PCI-MSI 333827-edge      iwlwifi: queue 3",
					QueueIndex: math.MaxInt64,
				},
			},
		},
		{
			name: "Shall return fast path IRQs only sorted by queue number",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:      40351          0          0          0   PCI-MSI 1572868-edge      eth0",
						92: "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
						93: "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
						94: "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
						95: "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        95,
					ProcLine:   "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
					QueueIndex: 0,
				},
				{
					Num:        94,
					ProcLine:   "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
					QueueIndex: 1,
				},
				{
					Num:        93,
					ProcLine:   "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
					QueueIndex: 2,
				},
				{
					Num:        92,
					ProcLine:   "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
					QueueIndex: 3,
				},
			},
		},
		{
			name: "Fdir fast path IRQs should be moved to the end of list",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:      40351          0          0          0   PCI-MSI 1572868-edge      eth0",
						92: "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
						93: "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
						94: "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
						95: "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
						96: "96:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-fdir-TxRx-0",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95, 96}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        95,
					ProcLine:   "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
					QueueIndex: 0,
				},
				{
					Num:        94,
					ProcLine:   "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
					QueueIndex: 1,
				},
				{
					Num:        93,
					ProcLine:   "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
					QueueIndex: 2,
				},
				{
					Num:        92,
					ProcLine:   "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
					QueueIndex: 3,
				},
				{
					Num:        96,
					ProcLine:   "96:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-fdir-TxRx-0",
					QueueIndex: math.MaxInt64,
				},
			},
		},
		{
			name:       "virtio",
			driverName: "virtio",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						24: "24:          0          0   PCI-MSI 65536-edge      virtio1-config",
						25: "25:        135          0   PCI-MSI 65537-edge      virtio1-input.2",
						26: "26:        221          0   PCI-MSI 65538-edge      virtio1-output.2",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{24, 25, 26}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        25,
					ProcLine:   "25:        135          0   PCI-MSI 65537-edge      virtio1-input.2",
					QueueIndex: 2,
				},
				{
					Num:        26,
					ProcLine:   "26:        221          0   PCI-MSI 65538-edge      virtio1-output.2",
					QueueIndex: 2,
				},
			},
		},
		{
			name:       "gvnic legacy",
			driverName: "gve",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						26: "26:        134          0   ITS-MSI   0 Edge      eth%d-ntfy-block.0",
						27: "27:          0        201   ITS-MSI   1 Edge      eth%d-ntfy-block.1",
						58: "58:          0          0   ITS-MSI  32 Edge      eth%d-mgmnt",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{26, 27, 58}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        26,
					ProcLine:   "26:        134          0   ITS-MSI   0 Edge      eth%d-ntfy-block.0",
					QueueIndex: 0,
				},
				{
					Num:        27,
					ProcLine:   "27:          0        201   ITS-MSI   1 Edge      eth%d-ntfy-block.1",
					QueueIndex: 0,
				},
			},
		},
		{
			name:       "gvnic",
			driverName: "gve",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						168: "168:          0          0 PCI-MSIX-0000:00:08.0  30-edge      gve-ntfy-blk0@pci:0000:00:08.0",
						169: "169:          0          0 PCI-MSIX-0000:00:08.0  31-edge      gve-ntfy-blk1@pci:0000:00:08.0",
						170: "170:          0          0 PCI-MSIX-0000:00:08.0  32-edge      gve-mgmnt@pci:0000:00:08.0",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{168, 169, 170}, nil
				},
			},
			want: []IrqInfoRes{
				{
					Num:        168,
					ProcLine:   "168:          0          0 PCI-MSIX-0000:00:08.0  30-edge      gve-ntfy-blk0@pci:0000:00:08.0",
					QueueIndex: 0,
				},
				{
					Num:        169,
					ProcLine:   "169:          0          0 PCI-MSIX-0000:00:08.0  31-edge      gve-ntfy-blk1@pci:0000:00:08.0",
					QueueIndex: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drivernameFunc := func(string) (string, error) { return tt.driverName, nil }
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   tt.irqProcFile,
				irqDeviceInfo: tt.irqDeviceInfo,
				ethtool:       &ethtoolMock{driverName: drivernameFunc},
				name:          "test0",
			}
			got, err := n.GetIRQs()
			require.NoError(t, err)
			require.Exactly(t, len(tt.want), len(got))

			for i := range got {
				require.Equal(t, tt.want[i].Num, got[i].Num)
				require.Equal(t, tt.want[i].ProcLine, got[i].ProcLine)
				require.Equal(t, tt.want[i].QueueIndex, got[i].QueueIndex())
			}
		})
	}
}

func Test_nic_GetMaxRxQueueCount(t *testing.T) {
	tests := []struct {
		name    string
		ethtool ethtool.EthtoolWrapper
		want    int
	}{
		{
			name: "Shall return correct max queues for ixgbe driver",
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ixgbe", nil
				},
			},
			want: 16,
		},
		{
			name: "Shall return max int when driver is unknown",
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "iwlwifi", nil
				},
			},
			want: MaxInt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			got, err := n.GetMaxRxQueueCount()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_nic_GetRxQueueCount(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		before        func(fs afero.Fs)
		want          int
	}{
		{
			name: "Shall return len(IRQ) when RPS is disabled and driver is not limiting queues number",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:...",
						92: "92:...",
						93: "93:...",
						94: "94:...",
						95: "95:...",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95}, nil
				},
			},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "iwlwifi", nil
				},
			},
			before: func(fs afero.Fs) {
			},
			want: 5,
		},
		{
			name:          "Shall return number of queues equal to number of rps_cpus files",
			irqProcFile:   &procFileMock{},
			irqDeviceInfo: &deviceInfoMock{},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ilwifi", nil
				},
			},
			before: func(fs afero.Fs) {
				for i := 0; i < 8; i++ {
					afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0o644)
				}
			},
			want: 8,
		},
		{
			name:          "Shall limit number of queues when they are limited by the driver",
			irqProcFile:   &procFileMock{},
			irqDeviceInfo: &deviceInfoMock{},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ixgbevf", nil
				},
			},
			before: func(fs afero.Fs) {
				for i := 0; i < 8; i++ {
					afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0o644)
				}
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			n := &nic{
				fs:            fs,
				irqProcFile:   tt.irqProcFile,
				irqDeviceInfo: tt.irqDeviceInfo,
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			tt.before(fs)
			got, err := n.GetRxQueueCount()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_nic_GetNTupleStatus(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		want          NTupleStatus
	}{
		{
			name: "Shall return not supported when iface does not support NTuples",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"other": true,
					}, nil
				},
			},
			want: NTupleNotSupported,
		},
		{
			name: "Shall return disabled when feature is present but disabled",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"ntuple": false,
					}, nil
				},
			},
			want: NTupleDisabled,
		},
		{
			name: "Shall return enabled when feature is present and enabled",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"ntuple": true,
					}, nil
				},
			},
			want: NTupleEnabled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			got, err := n.GetNTupleStatus()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_intelIrqToQueueIdx(t *testing.T) {
	irq := IrqInfo{
		Num:       92,
		ProcLine:  "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
		indexFunc: intelIrqToQueueIdx,
	}
	require.Equal(t, 3, irq.QueueIndex())
}

func Test_virtioIrqToQueueIdx(t *testing.T) {
	{
		irq := IrqInfo{
			Num:       27,
			ProcLine:  "27:          0         68   PCI-MSI 65539-edge      virtio8-input.1",
			indexFunc: virtioIrqToQueueIdx,
		}
		require.Equal(t, 1, irq.QueueIndex())
	}

	{
		irq := IrqInfo{
			Num:       27,
			ProcLine:  "27:          0         68   PCI-MSI 65539-edge      virtio77-output.18",
			indexFunc: virtioIrqToQueueIdx,
		}
		require.Equal(t, 18, irq.QueueIndex())
	}

	{
		irq := IrqInfo{
			Num:       27,
			ProcLine:  "27:          0         68   PCI-MSI 65539-edge      virtio1-config",
			indexFunc: virtioIrqToQueueIdx,
		}
		require.Equal(t, math.MaxInt64, irq.QueueIndex())
	}
}

func Test_gvnicIrqToQueueIdx(t *testing.T) {
	{
		irq := IrqInfo{
			Num:       26,
			ProcLine:  "26:        134          0   ITS-MSI   0 Edge      eth%d-ntfy-block.1",
			indexFunc: func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, 4) },
		}
		require.Equal(t, 1, irq.QueueIndex())
	}

	{
		irq := IrqInfo{
			Num:       26,
			ProcLine:  "26:        134          0   ITS-MSI   0 Edge      eth%d-ntfy-block.18",
			indexFunc: func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, 16) },
		}
		require.Equal(t, 2, irq.QueueIndex())
	}

	{
		irq := IrqInfo{
			Num:       26,
			ProcLine:  "26:        134          0   ITS-MSI   0 Edge      gve-ntfy-blk1@pci:0000:00:08.0",
			indexFunc: func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, 4) },
		}
		require.Equal(t, 1, irq.QueueIndex())
	}

	{
		irq := IrqInfo{
			Num:       26,
			ProcLine:  "26:        134          0   ITS-MSI   0 Edge      gve-ntfy-blk18@pci:0000:00:08.0",
			indexFunc: func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, 16) },
		}
		require.Equal(t, 2, irq.QueueIndex())
	}

	{

		irq := IrqInfo{
			Num:       26,
			ProcLine:  "26:        134          0   ITS-MSI   0 Edge      eth%d-mgmnt",
			indexFunc: func(irq IrqInfo) int { return gvnicIrqToQueueIdx(irq, 4) },
		}
		require.Equal(t, math.MaxInt64, irq.QueueIndex())
	}
}
