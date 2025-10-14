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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	et "github.com/safchain/ethtool"
	"go.uber.org/zap"
)

type EffectiveNicConfig struct {
	Mode                irq.Mode
	ComputationsCPUMask string
	IRQCPUMask          string
}

func GetEffectiveNicConfig(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners,
) (EffectiveNicConfig, error) {
	effectiveConfig := EffectiveNicConfig{}
	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	effectiveConfig.Mode, err = getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, t)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	effectiveConfig.IRQCPUMask, err = cpuMasks.CPUMaskForIRQs(effectiveConfig.Mode, effectiveCPUMask, t)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	effectiveConfig.ComputationsCPUMask, err = cpuMasks.CPUMaskForComputations(effectiveConfig.Mode, effectiveCPUMask, t)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	return effectiveConfig, nil
}

func GetDefaultMode(
	nic Nic, cpuMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners,
) (irq.Mode, error) {
	if nic.IsHwInterface() {
		numOfPUs, err := cpuMasks.GetNumberOfPUs(cpuMask)
		if err != nil {
			return "", err
		}

		// TODO: check
		//   - no cpuset specified in config
		//   - --smp is compatible
		var mode irq.Mode
		if numOfPUs >= uint(t.GetCoresPerDedicatedInterruptCore()) && t.GetAllowDedicatedInterruptMode() {
			mode = irq.Dedicated
		} else {
			mode = irq.Mq
		}

		zap.L().Sugar().Debugf("Using '%s' mode for '%s': '%d' PUs",
			mode, nic.Name(), numOfPUs)

		return mode, nil
	}

	if nic.IsBondIface() {
		defaultMode := irq.Mq
		slaves, err := nic.Slaves()
		if err != nil {
			return "", err
		}
		for _, slave := range slaves {
			slaveDefaultMode, err := GetDefaultMode(slave, cpuMask, cpuMasks, t)
			if err != nil {
				return "", err
			}
			if slaveDefaultMode == irq.Sq {
				defaultMode = irq.Sq
			} else if slaveDefaultMode == irq.SqSplit && defaultMode == irq.Mq {
				defaultMode = irq.SqSplit
			}
		}
		return defaultMode, nil
	}
	return "", fmt.Errorf("virtual device %s is not supported", nic.Name())
}

func getEffectiveMode(mode irq.Mode, nic Nic, effectiveCPUMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners) (irq.Mode, error) {
	var err error
	effectiveMode := mode
	if mode == irq.Default {
		effectiveMode, err = GetDefaultMode(nic, effectiveCPUMask, cpuMasks, t)
		if err != nil {
			return "", err
		}
	}
	return effectiveMode, nil
}

func GetRpsCPUMask(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners,
) (string, error) {
	if !t.GetAllowRpsRfsTuner() {
		return "0x0", nil
	}

	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return "", err
	}

	effectiveMode, err := getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, t)
	if err != nil {
		return "", err
	}

	queueCount, err := nic.GetRxQueueCount()
	if err != nil {
		return "", err
	}
	puCount, err := cpuMasks.GetNumberOfPUs(effectiveCPUMask)
	if err != nil {
		return "", err
	}

	// In MQ mode, with at least one hardware RX queue per core just disable RPS as it adds no benefit.
	if queueCount >= int(puCount) && effectiveMode == irq.Mq {
		return "0x0", nil
	}

	computationsCPUMask, err := cpuMasks.CPUMaskForComputations(
		effectiveMode, effectiveCPUMask, t)
	if err != nil {
		return "", err
	}
	return computationsCPUMask, nil
}

func GetHwInterfaceIRQsDistribution(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners,
) (map[int]string, error) {
	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return nil, err
	}

	effectiveMode, err := getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, t)
	if err != nil {
		return nil, err
	}

	maxRxQueues, err := nic.GetMaxRxQueueCount()
	if err != nil {
		return nil, err
	}

	allIRQs, err := nic.GetIRQs()
	if err != nil {
		return nil, err
	}

	irqCPUMask, err := cpuMasks.CPUMaskForIRQs(effectiveMode, effectiveCPUMask, t)
	if err != nil {
		return nil, err
	}

	rxQueues, err := nic.GetRxQueueCount()
	if err != nil {
		return nil, err
	}

	supportsIrqLowering, err := nic.SupportsRxTxQueueLowering()
	if err != nil {
		return nil, err
	}

	// Now we need to distribute IRQs to CPUs. This isn't entirely trivial as various
	// scenarios can exist and one needs to keep a few gotchas in mind.

	// hwloc-distrib distribution logic:

	// Assume you have an IRQ list of the following form

	// rx0
	// rx1
	// rx2
	// ...
	// rxN

	// and you ask hwloc-distrib to distribute this across cores. The effective outcome
	// will be that hwloc just splits the list in half and assigns the first half to
	// core 1 (rx0-N/2-1) and the second half to core 2 (N/2-N), i.e.: it won't
	// round-robin assign them.

	// When lowering RX-queues IRQs (as shown in /proc/interrupts) often
	// stay there and keep their name (virtio for example). On some platforms they stay
	// there but lose their name (AWS for example). The former is problematic as our
	// "fast path" IRQ filtering won't work then and we will still have them in the IRQ
	// list. Given the above hwloc logic this would be problematic in dedicated mode as
	// hwloc would assign all the earlier actually live  IRQs to a single of
	// possibly many dedicated interrupt cores while all the other ones would just get
	// dead IRQs.

	// To avoid this issue we split the hwloc-distrib process into two halfes. The
	// first half only distributes the alive IRQs while the second half
	// distributes the dead IRQs (I guess this later step could strictly be skipped).

	// Separate RX and TX IRQs. Certain drivers will expose two separate IRQs for
	// RX and TX (for example virtio). Note this is independent of whether ethtool will
	// show them as "combined" or separate (virtio uses "combined" in ethtool but has
	// separate IRQs).

	// This raises a problem with the above cut-off logic. Assume an IRQ list such as

	// rx0
	// tx0
	// rx1
	// tx1
	// ...
	// rxN
	// txN

	// if we cut off the list at the number of RX queues then we would lose half of the
	// IRQs that we care about. To avoid this issue we parse the actual queue index out
	// of the name (this will give rxN and txN the same index) and compare that against
	// the RX queues number.

	// Hence we don't enable RX queue lowering and the split IRQ assignments in the
	// following scenarios:

	//  - We are in MQ mode: this is to effectively keep legacy behavior and in MQ mode
	//  it's just safer to not do the split assignment and unlikely to make a
	//  difference anyway
	//  - It's an unknown driver: In this case we can't be sure we get the queue
	//  indexes right. It's very likely we get them wrong and hence assignment

	// There is an exception to the above where if we are on a driver that has broken
	// "RSS queue" behaviour (it announces more queues than actually support RSS).

	if (effectiveMode == irq.Mq || !supportsIrqLowering) && maxRxQueues >= len(allIRQs) {
		zap.L().Sugar().Debugf("Calculating distribution '%s' IRQs (not limiting by RX queues)", nic.Name())
		IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
			IrqInfosToIDs(allIRQs), irqCPUMask)
		if err != nil {
			return nil, err
		}
		return IRQsDistribution, nil
	}

	zap.L().Sugar().Debugf("Number of Rx queues for '%s' = '%d', max queues %d", nic.Name(), rxQueues, maxRxQueues)
	rxQueues = min(rxQueues, maxRxQueues)

	// Find the cut off for live IRQs
	irqCutOffIndex := getIrqCutOffIndex(allIRQs, rxQueues)

	zap.L().Sugar().Debugf("Cut-off-index: %d, sorted irq list: %v", irqCutOffIndex, IrqInfosToIDs(allIRQs))

	zap.L().Sugar().Debugf("Distributing '%s' IRQs handling Rx/Tx queues", nic.Name())
	IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		IrqInfosToIDs(allIRQs[0:irqCutOffIndex]), irqCPUMask)
	if err != nil {
		return nil, err
	}
	zap.L().Sugar().Debugf("Distributing rest of '%s' IRQs\n", nic.Name())
	restIRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		IrqInfosToIDs(allIRQs[irqCutOffIndex:]), irqCPUMask)
	if err != nil {
		return nil, err
	}
	for irq, mask := range restIRQsDistribution {
		IRQsDistribution[irq] = mask
	}
	return IRQsDistribution, nil
}

func getIrqCutOffIndex(allIRQs []IrqInfo, rxQueues int) int {
	// We find the last IRQ with queue index < rxQueues
	// In practice will often either be `rxQueues` (if all IRQs are either
	// combined or RX IRQs) or 2 * `rxQueues` (if all IRQs are split into RX and TX)
	for i, irq := range allIRQs {
		if irq.QueueIndex() >= rxQueues {
			return i
		}
	}

	return len(allIRQs)
}

// Returns the current ethtool channel config and the target (possibly lowered) channel config as required by the RX channel tuner.
func GetCurrentAndTargetChannels(
	nic Nic,
	mode irq.Mode,
	cpuMask string,
	cpuMasks irq.CPUMasks,
	t config.RpkNodeTuners,
	ethtool ethtool.EthtoolWrapper,
) (currentChannels et.Channels, targetChannels et.Channels, err error) {
	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	effectiveMode, err := getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, t)
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	irqMask, err := cpuMasks.CPUMaskForIRQs(effectiveMode, effectiveCPUMask, t)
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	puCount, err := cpuMasks.GetNumberOfPUs(irqMask)
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	currentChannels, err = ethtool.GetChannels(nic.Name())
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	targetChannels = currentChannels
	targetChannels.RxCount = min(currentChannels.MaxRx, uint32(puCount))
	targetChannels.TxCount = min(currentChannels.MaxTx, uint32(puCount))
	targetChannels.CombinedCount = min(currentChannels.MaxCombined, uint32(puCount))

	zap.L().Sugar().Debugf("Got current channels for '%s': %+v, target channels: %+v", nic.Name(), currentChannels, targetChannels)

	return currentChannels, targetChannels, nil
}

func CollectIRQs(nic Nic) ([]int, error) {
	var IRQs []int
	if nic.IsHwInterface() {
		nicIRQs, err := nic.GetIRQs()
		if err != nil {
			return nil, err
		}
		IRQs = append(IRQs, IrqInfosToIDs(nicIRQs)...)
	}
	if nic.IsBondIface() {
		slaves, err := nic.Slaves()
		if err != nil {
			return nil, err
		}
		for _, slave := range slaves {
			slaveIRQs, err := CollectIRQs(slave)
			if err != nil {
				return nil, err
			}
			IRQs = append(IRQs, slaveIRQs...)
		}
	}
	return IRQs, nil
}

func OneRPSQueueLimit(limits []string, nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CPUMasks, t config.RpkNodeTuners) (int, error) {
	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return 0, err
	}

	effectiveMode, err := getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, t)
	if err != nil {
		return 0, err
	}

	queueCount, err := nic.GetRxQueueCount()
	if err != nil {
		return 0, err
	}

	puCount, err := cpuMasks.GetNumberOfPUs(effectiveCPUMask)
	if err != nil {
		return 0, err
	}

	// In MQ mode, with at least one hardware RX queue per core just disable RFS as it adds no benefit.
	if queueCount >= int(puCount) && effectiveMode == irq.Mq {
		return 0, nil
	}
	if !t.GetAllowRpsRfsTuner() {
		return 0, nil
	}
	return RfsTableSize / len(limits), nil
}
