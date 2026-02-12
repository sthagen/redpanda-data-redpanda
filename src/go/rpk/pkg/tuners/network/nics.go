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
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	et "github.com/safchain/ethtool"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type EffectiveNicConfig struct {
	Mode                    irq.Mode
	ComputationsCPUMask     string
	ComputationsCPUMaskSize int
	IRQCPUMask              string
	IRQCPUMaskSize          int
}

func GetEffectiveNicConfig(
	nic Nic, mode irq.Mode, cpuMask string, cpuMasks irq.CPUMasks, rnc config.RpkNodeConfig,
) (EffectiveNicConfig, error) {
	effectiveConfig := EffectiveNicConfig{}
	effectiveCPUMask, err := cpuMasks.BaseCPUMask(cpuMask)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	effectiveConfig.Mode, err = getEffectiveMode(mode, nic, effectiveCPUMask, cpuMasks, rnc)
	if err != nil {
		return EffectiveNicConfig{}, err
	}

	effectiveConfig.IRQCPUMask, err = cpuMasks.CPUMaskForIRQs(effectiveConfig.Mode, effectiveCPUMask, rnc)
	if err != nil {
		return EffectiveNicConfig{}, err
	}
	irqPUs, err := cpuMasks.GetNumberOfPUs(effectiveConfig.IRQCPUMask)
	if err != nil {
		return EffectiveNicConfig{}, err
	}
	effectiveConfig.IRQCPUMaskSize = int(irqPUs)

	effectiveConfig.ComputationsCPUMask, err = cpuMasks.CPUMaskForComputations(effectiveConfig.Mode, effectiveCPUMask, rnc)
	if err != nil {
		return EffectiveNicConfig{}, err
	}
	computationPUs, err := cpuMasks.GetNumberOfPUs(effectiveConfig.ComputationsCPUMask)
	if err != nil {
		return EffectiveNicConfig{}, err
	}
	effectiveConfig.ComputationsCPUMaskSize = int(computationPUs)

	return effectiveConfig, nil
}

func checkAdditionalFlagsHasCpuset(additionalFlags map[string]string) bool {
	if _, hasCpuset := additionalFlags["cpuset"]; hasCpuset {
		return true
	}
	return false
}

func checkHasTunerCliCpuset(cpuMask string, cpuMasks irq.CPUMasks) (bool, error) {
	allMask, err := cpuMasks.GetAllCpusMask()
	if err != nil {
		return false, err
	}
	return allMask != cpuMask, nil
}

func maybeGetSmp(rnc config.RpkNodeConfig, additionalFlags map[string]string) (*int, error) {
	// Get either from additional flags or from the special rpk.smp config field
	// Note we don't need to handle the case where both are set as that is already rejected by rpk:start
	if smpStr, ok := additionalFlags["smp"]; ok {
		smp, err := strconv.Atoi(smpStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse smp value '%s': %w", smpStr, err)
		}
		return &smp, nil
	}
	return rnc.SMP, nil
}

func checkHasAcceptableSmp(numOfPUs int, additionalFlags map[string]string, rnc config.RpkNodeConfig) (bool, string, error) {
	// Handle custom SMP flags
	smp, err := maybeGetSmp(rnc, additionalFlags)
	if err != nil {
		return false, "", err
	}
	if smp != nil {
		// We differentiate between two cases here:

		// smp is slightly lowered, specifically we define "slightly" as lowered
		// less than amount of potential interrupt cores. This is to still allow
		// dedicated mode in case where people already lowered smp to give room
		// to the "OS". Likely in practice.
		potentialInterruptCores := int(math.Ceil(float64(numOfPUs) / float64(rnc.Tuners.GetCoresPerDedicatedInterruptCore())))
		if numOfPUs >= *smp && potentialInterruptCores >= (numOfPUs-*smp) {
			zap.L().Sugar().Debugf("allowing dedicated mode as smp is only slightly lowered - smp: %d, interrupt-cores: %d, num PUs: %d",
				*smp, potentialInterruptCores, numOfPUs)
			return true, "", nil
		}

		// smp is lowered by a larger amount. In this case we disallow dedicated
		// mode as we are likely not running on a RP-only system
		reason := fmt.Sprintf("smp: %d, interrupt-cores: %d, num PUs: %d", *smp, potentialInterruptCores, numOfPUs)

		return false, reason, nil
	}

	return true, "", nil
}

func canDefaultToDedicatedMode(numOfPUs int, cpuMask string, cpuMasks irq.CPUMasks, rnc config.RpkNodeConfig) (bool, error) {
	additionalFlags := config.ParseAdditionalStartFlags(rnc.AdditionalStartFlags)

	// Handle a custom cpuset, note this is incredibly unlikely to be the case in
	// practice as additional_start_flags isn't even documented and it's
	// unlikely anybody invokes the tuner with a custom cpuset.

	// If a custom cpuset is specified don't bother with defaulting to dedicated
	// mode. Just going to get it wrong and we are likely not running in a
	// RP-only environment.
	if checkAdditionalFlagsHasCpuset(additionalFlags) {
		zap.L().Sugar().Debugf("additional_start_flags contains cpuset, won't default to dedicated mode")
		return false, nil
	}

	// If the user specified a custom cpu-set to the tuner (likely via manual
	// non-systemd invocation) then we also bail out. We would still get things
	// right but it's safer to just require explicitly passing the mode in that
	// case as well.
	hasTunerCpuset, err := checkHasTunerCliCpuset(cpuMask, cpuMasks)
	if err != nil {
		return false, err
	}
	if hasTunerCpuset {
		zap.L().Sugar().Debugf("--cpu-mask passed on the tuner command line, won't default to dedicated mode")
		return false, nil
	}

	checkHasAcceptableSmp, reason, err := checkHasAcceptableSmp(numOfPUs, additionalFlags, rnc)
	if err != nil {
		return false, err
	}
	if !checkHasAcceptableSmp {
		zap.L().Sugar().Debugf("not defaulting to dedicated mode as smp is not acceptable: %s", reason)
		return false, nil
	}

	return true, nil
}

func GetDefaultMode(
	nic Nic, cpuMask string, cpuMasks irq.CPUMasks, rnc config.RpkNodeConfig,
) (irq.Mode, error) {
	numOfPUs, err := cpuMasks.GetNumberOfPUs(cpuMask)
	if err != nil {
		return "", err
	}

	canDoDedicated, err := canDefaultToDedicatedMode(int(numOfPUs), cpuMask, cpuMasks, rnc)
	if err != nil {
		return "", err
	}
	var mode irq.Mode
	if numOfPUs >= uint(rnc.Tuners.GetCoresPerDedicatedInterruptCore()) && rnc.Tuners.GetAllowDedicatedInterruptMode() && canDoDedicated {
		mode = irq.Dedicated
	} else {
		mode = irq.Mq
	}

	zap.L().Sugar().Debugf("Using '%s' mode for '%s': '%d' PUs",
		mode, nic.Name(), numOfPUs)

	return mode, nil
}

func checkDedicatedCompatibleConfig(cpuMask string, cpuMasks irq.CPUMasks, rnc config.RpkNodeConfig) error {
	additionalFlags := config.ParseAdditionalStartFlags(rnc.AdditionalStartFlags)
	numOfPUs, err := cpuMasks.GetNumberOfPUs(cpuMask)
	if err != nil {
		return err
	}

	// If there is a --cpuset specified in additional_start_flags we hard bail out. rpk:start doesn't support this
	if checkAdditionalFlagsHasCpuset(additionalFlags) {
		return fmt.Errorf("additional_start_flags contains cpuset which is incompatible with dedicated mode")
	}

	// Note, while for auto detection we don't allow a custom --cpu-set argument
	// we do for explicit invocation. The user likely knows what they are doing

	checkHasAcceptableSmp, reason, err := checkHasAcceptableSmp(int(numOfPUs), additionalFlags, rnc)
	if err != nil {
		return err
	}
	if !checkHasAcceptableSmp {
		return fmt.Errorf("dedicated mode is only compatible with smp values that are lowered by less than the number of potential interrupt cores: %s", reason)
	}

	return nil
}

func getEffectiveMode(mode irq.Mode, nic Nic, effectiveCPUMask string, cpuMasks irq.CPUMasks, rnc config.RpkNodeConfig) (irq.Mode, error) {
	var err error
	effectiveMode := mode
	switch mode {
	case irq.Default:
		effectiveMode, err = GetDefaultMode(nic, effectiveCPUMask, cpuMasks, rnc)
		if err != nil {
			return "", err
		}
	case irq.Dedicated:
		// dedicated specified via cli arg --mode dedicated
		err := checkDedicatedCompatibleConfig(effectiveCPUMask, cpuMasks, rnc)
		if err != nil {
			return "", err
		}
	}
	return effectiveMode, nil
}

func GetRpsCPUMask(
	nic Nic, effectiveConfig EffectiveNicConfig, rnc config.RpkNodeConfig,
) (string, error) {
	if !rnc.Tuners.GetAllowRpsRfsTuner() {
		return "0x0", nil
	}

	queueCount, err := nic.GetRxQueueCount()
	if err != nil {
		return "", err
	}

	// In MQ mode, with at least one hardware RX queue per core just disable RPS as it adds no benefit.
	if queueCount >= effectiveConfig.ComputationsCPUMaskSize && effectiveConfig.Mode == irq.Mq {
		return "0x0", nil
	}

	return effectiveConfig.ComputationsCPUMask, nil
}

func GetHwInterfaceIRQsDistribution(
	nic Nic, effectiveConfig EffectiveNicConfig, cpuMasks irq.CPUMasks,
) (map[int]string, error) {
	maxRxQueues, err := nic.GetMaxRxQueueCount()
	if err != nil {
		return nil, err
	}

	allIRQs, err := nic.GetIRQs()
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

	if (effectiveConfig.Mode == irq.Mq || !supportsIrqLowering) && maxRxQueues >= len(allIRQs) {
		zap.L().Sugar().Debugf("Calculating distribution '%s' IRQs (not limiting by RX queues)", nic.Name())
		IRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
			IrqInfosToIDs(allIRQs), effectiveConfig.IRQCPUMask)
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
		IrqInfosToIDs(allIRQs[0:irqCutOffIndex]), effectiveConfig.IRQCPUMask)
	if err != nil {
		return nil, err
	}
	zap.L().Sugar().Debugf("Distributing rest of '%s' IRQs", nic.Name())
	restIRQsDistribution, err := cpuMasks.GetIRQsDistributionMasks(
		IrqInfosToIDs(allIRQs[irqCutOffIndex:]), effectiveConfig.IRQCPUMask)
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
	effectiveConfig EffectiveNicConfig,
	ethtool ethtool.EthtoolWrapper,
) (currentChannels et.Channels, targetChannels et.Channels, err error) {
	currentChannels, err = ethtool.GetChannels(nic.Name())
	if err != nil {
		return et.Channels{}, et.Channels{}, err
	}

	targetChannels = currentChannels
	targetChannels.RxCount = min(currentChannels.MaxRx, uint32(effectiveConfig.IRQCPUMaskSize))
	targetChannels.TxCount = min(currentChannels.MaxTx, uint32(effectiveConfig.IRQCPUMaskSize))
	targetChannels.CombinedCount = min(currentChannels.MaxCombined, uint32(effectiveConfig.IRQCPUMaskSize))

	zap.L().Sugar().Debugf("Got current channels for '%s': %+v, target channels: %+v", nic.Name(), currentChannels, targetChannels)

	return currentChannels, targetChannels, nil
}

func CollectIRQs(nic Nic) ([]int, error) {
	var IRQs []int
	nicIRQs, err := nic.GetIRQs()
	if err != nil {
		return nil, err
	}
	IRQs = append(IRQs, IrqInfosToIDs(nicIRQs)...)
	return IRQs, nil
}

func OneRPSQueueLimit(limits []string, nic Nic, effectiveConfig EffectiveNicConfig, rnc config.RpkNodeConfig) (int, error) {
	queueCount, err := nic.GetRxQueueCount()
	if err != nil {
		return 0, err
	}

	// In MQ mode, with at least one hardware RX queue per core just disable RFS as it adds no benefit.
	if queueCount >= effectiveConfig.ComputationsCPUMaskSize && effectiveConfig.Mode == irq.Mq {
		return 0, nil
	}
	if !rnc.Tuners.GetAllowRpsRfsTuner() {
		return 0, nil
	}
	return RfsTableSize / len(limits), nil
}

func checkNic(nic Nic, nicMap map[string]Nic) {
	if _, exists := nicMap[nic.Name()]; exists {
		return
	}
	if !nic.IsHwInterface() && !nic.IsBondIface() {
		zap.L().Sugar().Debugf("Skipping tuning of '%s' virtual interface", nic.Name())
		return
	}

	// unroll bond slaves here such that we don't have to deal with later everywhere
	if nic.IsBondIface() {
		for _, slave := range nic.Slaves() {
			checkNic(slave, nicMap)
		}
	} else {
		nicMap[nic.Name()] = nic
	}
}

func MapInterfaces(interfaces []string, fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
) []Nic {
	nics := make(map[string]Nic)

	for _, iface := range interfaces {
		nic := NewNic(fs, irqProcFile, irqDeviceInfo, ethtool, iface)
		checkNic(nic, nics)
	}

	var nicsList []Nic
	for _, nic := range nics {
		nicsList = append(nicsList, nic)
	}
	return nicsList
}
