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

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/rpkutil"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type DeviceInfo interface {
	GetIRQs(irqConfigDir string, xenDeviceName string) ([]int, error)
}

func NewDeviceInfo(fs afero.Fs, procFile ProcFile) DeviceInfo {
	return &deviceInfo{
		procFile: procFile,
		fs:       fs,
	}
}

type deviceInfo struct {
	procFile ProcFile
	fs       afero.Fs
}

type EmptyMSIRQError struct {
	device string
}

func (e EmptyMSIRQError) Error() string {
	return fmt.Sprintf("device %q uses MSI IRQs but the list of msi_irqs in sysfs is empty; see https://github.com/redpanda-data/redpanda/issues/10838", e.device)
}

func (deviceInfo *deviceInfo) GetIRQs(
	irqConfigDir string, deviceName string,
) ([]int, error) {
	zap.L().Sugar().Debugf("Reading IRQs of '%s', with deviceInfo name pattern '%s'", irqConfigDir, deviceName)
	msiIRQsDirName := path.Join(irqConfigDir, "msi_irqs")
	var irqs []int
	if exists, _ := afero.Exists(deviceInfo.fs, msiIRQsDirName); exists {
		zap.L().Sugar().Debugf("Device '%s' uses MSI IRQs", irqConfigDir)
		files := rpkutil.ListFilesInPath(deviceInfo.fs, msiIRQsDirName)
		for _, file := range files {
			irq, err := strconv.Atoi(file)
			if err != nil {
				return nil, err
			}
			irqs = append(irqs, irq)
		}
		if len(irqs) == 0 {
			return nil, &EmptyMSIRQError{irqConfigDir}
		}
	} else {
		irqFileName := path.Join(irqConfigDir, "irq")
		if exists, _ := afero.Exists(deviceInfo.fs, irqFileName); exists {
			zap.L().Sugar().Debugf("Device '%s' uses INT#x IRQs", irqConfigDir)
			lines, err := rpkutil.ReadFileLines(deviceInfo.fs, irqFileName)
			if err != nil {
				return nil, err
			}
			for _, rawLine := range lines {
				irq, err := strconv.Atoi(strings.TrimSpace(rawLine))
				if err != nil {
					return nil, err
				}
				irqs = append(irqs, irq)
			}
		} else {
			modAliasFileName, err := findModalias(deviceInfo.fs, irqConfigDir)
			if err != nil {
				return nil, fmt.Errorf("unable to find device info in %q: %v", irqConfigDir, err)
			}
			lines, err := rpkutil.ReadFileLines(deviceInfo.fs, modAliasFileName)
			if err != nil {
				return nil, err
			}
			modAlias := lines[0]
			zap.L().Sugar().Debugf("Found modalias with name %s", modAlias)
			irqProcFileLines, err := deviceInfo.procFile.GetIRQProcFileLinesMap()
			if err != nil {
				return nil, err
			}
			if strings.Contains(modAlias, "virtio") {
				zap.L().Sugar().Debugf("Device '%s' is a virtio device type", irqConfigDir)
				fileNames := rpkutil.ListFilesInPath(deviceInfo.fs, path.Join(irqConfigDir, "driver"))
				for _, name := range fileNames {
					if strings.Contains(name, "virtio") {
						irqs = append(irqs,
							deviceInfo.getIRQsForLinesMatching(name, irqProcFileLines)...)
					}
				}
			} else if strings.Contains(modAlias, "xen:") {
				zap.L().Sugar().Debugf("Reading '%s' device IRQs from /proc/interrupts", irqConfigDir)
				irqs = deviceInfo.getIRQsForLinesMatching(deviceName, irqProcFileLines)
			} else {
				zap.L().Sugar().Debugf("No modalias support for %s", modAlias)
			}
		}
	}

	sort.Ints(irqs)

	zap.L().Sugar().Debugf("DeviceInfo '%s' IRQs '%v'", irqConfigDir, irqs)
	return irqs, nil
}

func (*deviceInfo) getIRQsForLinesMatching(
	pattern string, irqToProcLineMap map[int]string,
) []int {
	var irqs []int
	for irq, line := range irqToProcLineMap {
		if strings.Contains(line, pattern) {
			irqs = append(irqs, irq)
		}
	}
	return irqs
}

// findModalias recursively tries to find the modalias file in all the parent
// directories until we reach /sys/devices or root. It returns the filepath
// to the modalias file.
func findModalias(fs afero.Fs, dir string) (string, error) {
	if dir == "/sys/devices" || dir == "/" {
		return "", fmt.Errorf("unable to find modalias")
	}

	modAliasFileName := path.Join(dir, "modalias")
	if exists, _ := afero.Exists(fs, modAliasFileName); exists {
		return modAliasFileName, nil
	}

	return findModalias(fs, filepath.Dir(dir))
}
