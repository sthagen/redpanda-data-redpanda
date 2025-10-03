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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"go.uber.org/zap"
)

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

	if maxRxQueues >= len(allIRQs) {
		zap.L().Sugar().Debugf("Calculating distribution '%s' IRQs", nic.Name())
		IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
			allIRQs, irqCPUMask)
		if err != nil {
			return nil, err
		}
		return IRQsDistribution, nil
	}

	rxQueues, err := nic.GetRxQueueCount()
	if err != nil {
		return nil, err
	}
	zap.L().Sugar().Debugf("Number of Rx queues for '%s' = '%d'", nic.Name(), rxQueues)
	fmt.Printf("Distributing '%s' IRQs handling Rx queues\n", nic.Name())
	IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		allIRQs[0:rxQueues], irqCPUMask)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Distributing rest of '%s' IRQs\n", nic.Name())
	restIRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		allIRQs[rxQueues:], irqCPUMask)
	if err != nil {
		return nil, err
	}
	for irq, mask := range restIRQsDistribution {
		IRQsDistribution[irq] = mask
	}
	return IRQsDistribution, nil
}

func CollectIRQs(nic Nic) ([]int, error) {
	var IRQs []int
	if nic.IsHwInterface() {
		nicIRQs, err := nic.GetIRQs()
		if err != nil {
			return nil, err
		}
		IRQs = append(IRQs, nicIRQs...)
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
	return RfsTableSize / len(limits), nil
}
