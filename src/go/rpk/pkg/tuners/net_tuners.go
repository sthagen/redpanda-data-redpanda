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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/spf13/afero"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type CpusetConfig struct {
	IrqMode            irq.Mode `yaml:"irq_mode,omitempty" json:"irq_mode"`
	RedpandaCpuset     string   `yaml:"redpanda_cpuset,omitempty" json:"redpanda_cpuset"`
	RedpandaCpusetSize int      `yaml:"redpanda_cpuset_size,omitempty" json:"redpanda_cpuset_size"`
	IrqCpuset          string   `yaml:"interrupts_cpuset,omitempty" json:"interrupts_cpuset"`
	IrqCpusetSize      int      `yaml:"interrupts_cpuset_size,omitempty" json:"interrupts_cpuset_size"`
}

type NetTunerConfig struct {
	Cpusets *CpusetConfig `yaml:"cpusets,omitempty" json:"cpusets"`
}

func NewNetTuner(
	mode irq.Mode,
	t config.RpkNodeTuners,
	cpuMask string,
	interfaces []string,
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CPUMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	ethtool ethtool.EthtoolWrapper,
	executor executors.Executor,
	proc os.Proc,
) Tunable {
	factory := NewNetTunersFactory(
		fs, t, irqProcFile, irqDeviceInfo, ethtool, irqBalanceService, cpuMasks, executor, proc)
	return NewAggregatedTunable(
		[]Tunable{
			factory.NewAllNicsSameModeTuner(interfaces, mode, cpuMask),
			// RX/TX queue count tuner should always be first in order as others will re-read queue counts
			factory.NewRxTxQueueCountTuner(interfaces, mode, cpuMask),
			factory.NewNICsBalanceServiceTuner(interfaces),
			factory.NewNICsIRQsAffinityTuner(interfaces, mode, cpuMask),
			// Write out net tuner interrupt config once we have successfully tuned IRQ config
			factory.NewInterruptConfigFileTuner(interfaces, mode, cpuMask),
			factory.NewNICsRpsTuner(interfaces, mode, cpuMask),
			factory.NewNICsRfsTuner(interfaces, mode, cpuMask),
			factory.NewNICsNTupleTuner(interfaces),
			factory.NewNICsXpsTuner(interfaces),
			factory.NewRfsTableSizeTuner(),
			factory.NewListenBacklogTuner(),
			factory.NewSynBacklogTuner(),
		})
}

type NetTunersFactory interface {
	NewAllNicsSameModeTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewRxTxQueueCountTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsBalanceServiceTuner(interfaces []string) Tunable
	NewNICsIRQsAffinityTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewInterruptConfigFileTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsRpsTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsRfsTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsNTupleTuner(interfaces []string) Tunable
	NewNICsXpsTuner(interfaces []string) Tunable
	NewRfsTableSizeTuner() Tunable
	NewListenBacklogTuner() Tunable
	NewSynBacklogTuner() Tunable
}

type netTunersFactory struct {
	fs              afero.Fs
	t               config.RpkNodeTuners
	irqProcFile     irq.ProcFile
	irqDeviceInfo   irq.DeviceInfo
	ethtool         ethtool.EthtoolWrapper
	balanceService  irq.BalanceService
	cpuMasks        irq.CPUMasks
	checkersFactory NetCheckersFactory
	executor        executors.Executor
	proc            os.Proc
}

func NewNetTunersFactory(
	fs afero.Fs,
	t config.RpkNodeTuners,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CPUMasks,
	executor executors.Executor,
	proc os.Proc,
) NetTunersFactory {
	return &netTunersFactory{
		fs:             fs,
		t:              t,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
		executor:       executor,
		proc:           proc,
		checkersFactory: NewNetCheckersFactory(
			fs, t, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks),
	}
}

type NicsEqualTunable struct {
	f          *netTunersFactory
	interfaces []string
	mode       irq.Mode
	cpuMask    string
}

func (*NicsEqualTunable) CheckIfSupported() (bool, string) {
	return true, ""
}

func (net *NicsEqualTunable) Tune() TuneResult {
	var currentEffectiveConfig *network.EffectiveNicConfig

	for _, iface := range net.interfaces {
		nic := network.NewNic(net.f.fs, net.f.irqProcFile, net.f.irqDeviceInfo, net.f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			zap.L().Sugar().Debugf("Skipping tuning of '%s' virtual interface", nic.Name())
			continue
		}

		effectiveConfig, err := network.GetEffectiveNicConfig(nic, net.mode, net.cpuMask, net.f.cpuMasks, net.f.t)
		if err != nil {
			return NewTuneError(err)
		}

		zap.L().Sugar().Debugf("interface %s: effective config: %v", nic.Name(), effectiveConfig)
		if currentEffectiveConfig == nil {
			currentEffectiveConfig = &effectiveConfig
		} else {
			if *currentEffectiveConfig != effectiveConfig {
				return NewTuneError(fmt.Errorf("interfaces %s has different effective configuration than the rest: %v vs %v",
					nic.Name(), *currentEffectiveConfig, effectiveConfig))
			}
		}
	}
	return NewTuneResult(false)
}

// This is a meta tuner that checks that all given NICs use the same mode and IRQ masks.
// It's really only needed like this because of the way the different subtuners are implemented.
// Once we rewrite everything to be a single function this can just be at the top and won't be a separate "tuner".
func (f *netTunersFactory) NewAllNicsSameModeTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable {
	tuneable := NicsEqualTunable{f: f, interfaces: interfaces, mode: mode, cpuMask: cpuMask}
	return &tuneable
}

func (f *netTunersFactory) NewRxTxQueueCountTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRxTxQueueCountChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' queue counts", nic.Name()))
			if !f.t.GetAllowRxTxQueueTuner() {
				zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as it's disabled by configuration")
				return NewTuneResult(false)
			}

			supportsIrqLowering, err := nic.SupportsRxTxQueueLowering()
			if err != nil {
				return NewTuneError(err)
			}
			if !supportsIrqLowering {
				zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as using an unknown driver")
				return NewTuneResult(false)
			}

			_, targetChannels, err := network.GetCurrentAndTargetChannels(nic, mode, cpuMask, f.cpuMasks, f.t, f.ethtool)
			if err != nil {
				return NewTuneError(err)
			}

			_, err = f.ethtool.SetChannels(nic.Name(), targetChannels)
			if err != nil {
				return NewTuneError(err)
			}

			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsBalanceServiceTuner(
	interfaces []string,
) Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewNicIRQBalanceChecker(interfaces),
		func() TuneResult {
			var IRQs []int
			for _, ifaceName := range interfaces {
				nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, ifaceName)
				nicIRQs, err := network.CollectIRQs(nic)
				if err != nil {
					return NewTuneError(err)
				}
				zap.L().Sugar().Debugf("%s interface IRQs: %v", nic.Name(), nicIRQs)
				IRQs = append(IRQs, nicIRQs...)
			}
			err := f.balanceService.BanIRQsAndRestart(IRQs)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewNICsIRQsAffinityTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicIRQAffinityChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' IRQs affinity", nic.Name()))
			dist, err := network.GetHwInterfaceIRQsDistribution(nic, mode, cpuMask, f.cpuMasks, f.t)
			if err != nil {
				return NewTuneError(err)
			}
			f.cpuMasks.DistributeIRQs(dist)
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) getNetTunerConfig(nic network.Nic, mode irq.Mode, cpuMask string) (NetTunerConfig, error) {
	networkConfig, err := network.GetEffectiveNicConfig(nic, mode, cpuMask, f.cpuMasks, f.t)
	if err != nil {
		return NetTunerConfig{}, err
	}

	// All the below transformations we could also do in rpk:start but we just
	// do them here to avoid having to invoke hwloc again then.

	redpandaCpusetListForm, err := f.cpuMasks.MaskToListFormat(networkConfig.ComputationsCPUMask)
	if err != nil {
		return NetTunerConfig{}, err
	}
	redpandaSize, err := f.cpuMasks.GetNumberOfPUs(networkConfig.ComputationsCPUMask)
	if err != nil {
		return NetTunerConfig{}, err
	}
	irqCpusetListForm, err := f.cpuMasks.MaskToListFormat(networkConfig.IRQCPUMask)
	if err != nil {
		return NetTunerConfig{}, err
	}
	irqSize, err := f.cpuMasks.GetNumberOfPUs(networkConfig.IRQCPUMask)
	if err != nil {
		return NetTunerConfig{}, err
	}

	config := NetTunerConfig{
		Cpusets: &CpusetConfig{
			IrqMode:            networkConfig.Mode,
			RedpandaCpuset:     redpandaCpusetListForm,
			RedpandaCpusetSize: int(redpandaSize),
			IrqCpuset:          irqCpusetListForm,
			IrqCpusetSize:      int(irqSize),
		},
	}
	return config, nil
}

func (f *netTunersFactory) NewInterruptConfigFileTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable {
	if len(interfaces) == 0 {
		// No interfaces, nothing to do
		return NewAggregatedTunable([]Tunable{})
	}

	// In the AllNicsSameMode we have already checked that the mode and
	// config for all NICs is the same so we can just use the first here
	nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, interfaces[0])

	tunable := checkedTunable{}
	tunable.tuneAction = func() TuneResult {
		zap.L().Sugar().Debugf(out.WithLogBanner("Creating tuner config file"))

		config, err := f.getNetTunerConfig(nic, mode, cpuMask)
		if err != nil {
			return NewTuneError(err)
		}

		if config.Cpusets.IrqMode == irq.Mq {
			// Only do in dedicated mode to keep legacy behaviour otherwise. Remove the file if it exists.
			zap.L().Sugar().Debugf("Not writing net tuner config as irq mode is %s", config.Cpusets.IrqMode)

			exists, err := afero.Exists(f.fs, network.NetTunerConfigFile)
			if err != nil {
				return NewTuneError(err)
			}
			if exists {
				err := f.fs.Remove(network.NetTunerConfigFile)
				if err != nil {
					return NewTuneError(fmt.Errorf("failed to remove existing net tuner config file %s: %w", network.NetTunerConfigFile, err))
				}
			}

			return NewTuneResult(false)
		}

		marshalled, err := yaml.Marshal(&config)
		if err != nil {
			return NewTuneError(err)
		}

		err = f.executor.Execute(
			commands.NewWriteFileCmd(f.fs, network.NetTunerConfigFile, string(marshalled)))
		if err != nil {
			return NewTuneError(fmt.Errorf("failed to write to net tuner config file %s: %w", network.NetTunerConfigFile, err))
		}

		return NewTuneResult(false)
	}
	tunable.checker = NewEqualityChecker(
		NetTunerConfigFileChecker,
		"Net tuner config file correct",
		Warning,
		true,
		func() (interface{}, error) {
			targetConfig, err := f.getNetTunerConfig(nic, mode, cpuMask)
			if err != nil {
				return false, err
			}

			exists, err := afero.Exists(f.fs, network.NetTunerConfigFile)
			if err != nil {
				return false, err
			}

			if targetConfig.Cpusets.IrqMode != irq.Dedicated {
				// If in MQ mode we still need to run the tuner such that it removes the file if it exists
				return !exists, nil
			}

			if !exists {
				return false, nil
			}

			content, err := afero.ReadFile(f.fs, network.NetTunerConfigFile)
			if err != nil {
				return false, err
			}

			currentConfig := NetTunerConfig{}
			err = yaml.Unmarshal(content, &currentConfig)
			if err != nil {
				return false, err
			}

			if currentConfig != targetConfig {
				zap.L().Sugar().Debugf("Current config %+v is different than target %+v", currentConfig, targetConfig)
				return false, nil
			}

			return true, nil
		},
	)
	tunable.supportedAction = func() (bool, string) { return true, "" }
	tunable.disablePostTuneCheck = f.executor.IsLazy()

	return &tunable
}

func (f *netTunersFactory) NewNICsRpsTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRpsSetChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' RPS", nic.Name()))
			rpsCPUs, err := nic.GetRpsCPUFiles()
			if err != nil {
				return NewTuneError(err)
			}
			rpsMask, err := network.GetRpsCPUMask(nic, mode, cpuMask, f.cpuMasks, f.t)
			if err != nil {
				return NewTuneError(err)
			}
			for _, rpsCPUFile := range rpsCPUs {
				err := f.cpuMasks.SetMask(rpsCPUFile, rpsMask)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsRfsTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRfsChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' RFS", nic.Name()))
			limits, err := nic.GetRpsLimitFiles()
			if err != nil {
				return NewTuneError(err)
			}
			queueLimit, err := network.OneRPSQueueLimit(limits, nic, mode, cpuMask, f.cpuMasks, f.t)
			if err != nil {
				return NewTuneError(err)
			}
			for _, limitFile := range limits {
				err := f.writeIntToFile(limitFile, queueLimit)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsNTupleTuner(interfaces []string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicNTupleChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' NTuple", nic.Name()))
			ntupleFeature := map[string]bool{"ntuple": true}
			err := f.executor.Execute(
				commands.NewEthtoolChangeCmd(f.ethtool, nic.Name(), ntupleFeature))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsXpsTuner(interfaces []string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicXpsChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' XPS", nic.Name()))
			xpsCPUFiles, err := nic.GetXpsCPUFiles()
			if err != nil {
				return NewTuneError(err)
			}
			masks, err := f.cpuMasks.GetDistributionMasks(uint(len(xpsCPUFiles)))
			if err != nil {
				return NewTuneError(err)
			}
			for i, mask := range masks {
				err := f.cpuMasks.SetMask(xpsCPUFiles[i], mask)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewRfsTableSizeTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewRfsTableSizeChecker(),
		func() TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning RFS table size"))
			err := f.executor.Execute(
				commands.NewSysctlSetCmd(
					network.RfsTableSizeProperty, fmt.Sprint(network.RfsTableSize)))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewListenBacklogTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewListenBacklogChecker(),
		func() TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning connections listen backlog size"))
			err := f.writeIntToFile(network.ListenBacklogFile, network.ListenBacklogSize)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewSynBacklogTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewSynBacklogChecker(),
		func() TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning SYN backlog size"))
			err := f.writeIntToFile(network.SynBacklogFile, network.SynBacklogSize)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) writeIntToFile(file string, value int) error {
	return f.executor.Execute(
		commands.NewWriteFileCmd(f.fs, file, fmt.Sprint(value)))
}

func (f *netTunersFactory) tuneNonVirtualInterfaces(
	interfaces []string,
	checkerCreator func(network.Nic) Checker,
	tuneAction func(network.Nic) TuneResult,
	supportedAction func() (bool, string),
) Tunable {
	var tunables []Tunable
	for _, iface := range interfaces {
		nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			zap.L().Sugar().Debugf("Skipping tuning of '%s' virtual interface", nic.Name())
			continue
		}
		tunables = append(tunables, NewCheckedTunable(
			checkerCreator(nic),
			func() TuneResult {
				return tuneInterface(nic, tuneAction)
			},
			supportedAction,
			f.executor.IsLazy(),
		))
	}
	return NewAggregatedTunable(tunables)
}

func tuneInterface(
	nic network.Nic, tuneAction func(network.Nic) TuneResult,
) TuneResult {
	if nic.IsHwInterface() {
		return tuneAction(nic)
	}

	if nic.IsBondIface() {
		slaves, err := nic.Slaves()
		if err != nil {
			return NewTuneError(err)
		}
		for _, slave := range slaves {
			return tuneInterface(slave, tuneAction)
		}
	}

	return NewTuneResult(false)
}
