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
	"strconv"

	"github.com/lorenzosaino/go-sysctl"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/rpkutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type NetCheckersFactory interface {
	NewNicRxTxQueueCountCheckers(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) []Checker
	NewNicRxTxQueueCountChecker(nic network.Nic, effectiveConfig network.EffectiveNicConfig) Checker
	NewNicIRQBalanceChecker(interfaces []network.Nic) Checker
	NewNicIRQAffinityCheckers(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) []Checker
	NewNicIRQAffinityChecker(nic network.Nic, effectiveConfig network.EffectiveNicConfig) Checker
	NewNicRpsSetCheckers(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) []Checker
	NewNicRpsSetChecker(nic network.Nic, effectiveConfig network.EffectiveNicConfig) Checker
	NewNicRfsCheckers(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) []Checker
	NewNicRfsChecker(nic network.Nic, effectiveConfig network.EffectiveNicConfig) Checker
	NewNicNTupleCheckers(interfaces []network.Nic) []Checker
	NewNicNTupleChecker(nic network.Nic) Checker
	NewNicXpsCheckers(interfaces []network.Nic) []Checker
	NewNicXpsChecker(nic network.Nic) Checker
	NewRfsTableSizeChecker() Checker
	NewListenBacklogChecker() Checker
	NewSynBacklogChecker() Checker
}

type netCheckersFactory struct {
	fs             afero.Fs
	rnc            config.RpkNodeConfig
	irqProcFile    irq.ProcFile
	irqDeviceInfo  irq.DeviceInfo
	ethtool        ethtool.EthtoolWrapper
	balanceService irq.BalanceService
	cpuMasks       irq.CPUMasks
}

func NewNetCheckersFactory(
	fs afero.Fs,
	rnc config.RpkNodeConfig,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CPUMasks,
) NetCheckersFactory {
	return &netCheckersFactory{
		fs:             fs,
		rnc:            rnc,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
	}
}

func (f *netCheckersFactory) NewNicRxTxQueueCountChecker(
	nic network.Nic, effectiveConfig network.EffectiveNicConfig,
) Checker {
	return NewEqualityChecker(
		NicRxTxQueueCountChecker,
		fmt.Sprintf("NIC %s RX/TX queue count set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				if !f.rnc.Tuners.GetAllowRxTxQueueTuner() {
					zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as it's disabled by configuration")
					return true, nil
				}

				supportsIrqLowering, err := currentNic.SupportsRxTxQueueLowering()
				if err != nil {
					return false, err
				}
				if !supportsIrqLowering {
					zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as using an unknown driver")
					return true, nil
				}

				currentChannels, targetChannels, err := network.GetCurrentAndTargetChannels(currentNic, effectiveConfig, f.ethtool)
				if err != nil {
					return false, err
				}

				rxCheck := currentChannels.RxCount == targetChannels.RxCount
				txCheck := currentChannels.TxCount == targetChannels.TxCount
				combinedCheck := currentChannels.CombinedCount == targetChannels.CombinedCount

				// We need all to be true because for the not in use one the check will always be true (0 == 0)
				return rxCheck && txCheck && combinedCheck, nil
			})
		})
}

func (f *netCheckersFactory) NewNicRxTxQueueCountCheckers(
	interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig,
) []Checker {
	return f.forInterfaces(interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicRxTxQueueCountChecker(nic, effectiveConfig)
		})
}

func (f *netCheckersFactory) NewNicIRQBalanceChecker(
	interfaces []network.Nic,
) Checker {
	return NewEqualityChecker(
		NicIRQBalanceChecker,
		"NIC IRQs excluded in irqbalance",
		Warning,
		true,
		func() (interface{}, error) {
			var IRQs []int
			for _, nic := range interfaces {
				nicIRQs, err := network.CollectIRQs(nic)
				if err != nil {
					return false, err
				}
				IRQs = append(IRQs, nicIRQs...)
			}
			return irq.AreIRQsStaticallyAssigned(IRQs, f.balanceService)
		},
	)
}

func (f *netCheckersFactory) NewNicIRQAffinityChecker(
	nic network.Nic, effectiveConfig network.EffectiveNicConfig,
) Checker {
	return NewEqualityChecker(
		NicIRQsAffinitChecker,
		fmt.Sprintf("NIC %s IRQ affinity set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				dist, err := network.GetHwInterfaceIRQsDistribution(
					currentNic, effectiveConfig, f.cpuMasks)
				if err != nil {
					return false, err
				}
				for IRQ, mask := range dist {
					readMask, err := f.cpuMasks.ReadIRQMask(IRQ)
					if err != nil {
						return false, err
					}
					eq, err := irq.MasksEqual(readMask, mask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		})
}

func (f *netCheckersFactory) NewNicIRQAffinityCheckers(
	interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig,
) []Checker {
	return f.forInterfaces(interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicIRQAffinityChecker(nic, effectiveConfig)
		})
}

func (f *netCheckersFactory) NewNicRpsSetCheckers(
	interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig,
) []Checker {
	return f.forInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicRpsSetChecker(nic, effectiveConfig)
		})
}

func (f *netCheckersFactory) NewNicRpsSetChecker(
	nic network.Nic, effectiveConfig network.EffectiveNicConfig,
) Checker {
	return NewEqualityChecker(
		NicRpsChecker,
		fmt.Sprintf("NIC %s RPS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				rpsCPUs, err := currentNic.GetRpsCPUFiles()
				if err != nil {
					return false, err
				}
				rfsMask, err := network.GetRpsCPUMask(currentNic, effectiveConfig, f.rnc)
				if err != nil {
					return false, err
				}
				for _, rpsCPU := range rpsCPUs {
					readMask, err := f.cpuMasks.ReadMask(rpsCPU)
					if err != nil {
						return false, err
					}
					eq, err := irq.MasksEqual(readMask, rfsMask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicRfsCheckers(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) []Checker {
	return f.forInterfaces(interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicRfsChecker(nic, effectiveConfig)
		})
}

func (f *netCheckersFactory) NewNicRfsChecker(nic network.Nic, effectiveConfig network.EffectiveNicConfig) Checker {
	return NewEqualityChecker(
		NicRfsChecker,
		fmt.Sprintf("NIC %s RFS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				limits, err := currentNic.GetRpsLimitFiles()
				if err != nil {
					return false, err
				}
				queueLimit, err := network.OneRPSQueueLimit(limits, currentNic, effectiveConfig, f.rnc)
				if err != nil {
					return false, err
				}
				for _, limitFile := range limits {
					setLimit, err := rpkutil.ReadIntFromFile(f.fs, limitFile)
					if err != nil {
						return false, err
					}
					if setLimit != queueLimit {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicNTupleCheckers(
	interfaces []network.Nic,
) []Checker {
	return f.forInterfaces(interfaces, f.NewNicNTupleChecker)
}

func (*netCheckersFactory) NewNicNTupleChecker(nic network.Nic) Checker {
	return NewEqualityChecker(
		NicNTupleChecker,
		fmt.Sprintf("NIC %s NTuple set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				nTupleStatus, err := currentNic.GetNTupleStatus()
				if err != nil {
					return false, err
				}
				if nTupleStatus == network.NTupleDisabled {
					return false, nil
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicXpsCheckers(interfaces []network.Nic) []Checker {
	return f.forInterfaces(interfaces, f.NewNicXpsChecker)
}

func (f *netCheckersFactory) NewNicXpsChecker(nic network.Nic) Checker {
	return NewEqualityChecker(
		NicXpsChecker,
		fmt.Sprintf("NIC %s XPS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				xpsCPUFiles, err := currentNic.GetXpsCPUFiles()
				if err != nil {
					return false, err
				}
				masks, err := f.cpuMasks.GetDistributionMasks(uint(len(xpsCPUFiles)))
				if err != nil {
					return false, err
				}
				for i, mask := range masks {
					if exists, _ := afero.Exists(f.fs, xpsCPUFiles[i]); !exists {
						continue
					}
					readMask, err := f.cpuMasks.ReadMask(xpsCPUFiles[i])
					if err != nil {
						continue
					}
					eq, err := irq.MasksEqual(readMask, mask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (*netCheckersFactory) NewRfsTableSizeChecker() Checker {
	return NewIntChecker(
		RfsTableEntriesChecker,
		"RFS Table entries",
		Warning,
		func(current int) bool {
			return current >= network.RfsTableSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.RfsTableSize)
		},
		func() (int, error) {
			value, err := sysctl.Get(network.RfsTableSizeProperty)
			if err != nil {
				return 0, err
			}
			return strconv.Atoi(value)
		},
	)
}

func (f *netCheckersFactory) NewListenBacklogChecker() Checker {
	return NewIntChecker(
		ListenBacklogChecker,
		"Connections listen backlog size",
		Warning,
		func(current int) bool {
			return current >= network.ListenBacklogSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.ListenBacklogSize)
		},
		func() (int, error) {
			return rpkutil.ReadIntFromFile(f.fs, network.ListenBacklogFile)
		},
	)
}

func (f *netCheckersFactory) NewSynBacklogChecker() Checker {
	return NewIntChecker(
		SynBacklogChecker,
		"Max syn backlog size",
		Warning,
		func(current int) bool {
			return current >= network.SynBacklogSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.SynBacklogSize)
		},
		func() (int, error) {
			return rpkutil.ReadIntFromFile(f.fs, network.SynBacklogFile)
		},
	)
}

func isSet(
	nic network.Nic, hwCheckFunction func(network.Nic) (bool, error),
) (bool, error) {
	return hwCheckFunction(nic)
}

func (*netCheckersFactory) forInterfaces(
	interfaces []network.Nic, checkerFactory func(network.Nic) Checker,
) []Checker {
	var chkrs []Checker
	for _, iface := range interfaces {
		chkrs = append(chkrs, checkerFactory(iface))
	}
	return chkrs
}
