// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"fmt"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"go.uber.org/zap"
)

func UnameAndDistro(timeout time.Duration) (string, error) {
	res, err := uname()
	if err != nil {
		return "", err
	}
	cmd := "lsb_release"
	p := osutil.NewProc()
	ls, err := p.RunWithSystemLdPath(timeout, cmd, "-d", "-s")
	if err != nil {
		zap.L().Sugar().Debugf("%s failed", cmd)
	}
	if len(ls) == 0 {
		zap.L().Sugar().Debugf("%s didn't return any output", cmd)
	} else {
		res += " " + ls[0]
	}
	return res, nil
}

// Returns a string representation of the input in terms of Gib/Mib/Kib or bits
// depending on how large the input is.
func BitsToHuman(bytes float64) string {
	bits := bytes * 8
	asGb := bits / 1000000000
	asMb := bits / 1000000
	asKb := bits / 1000
	if asGb > 1.0 {
		return fmt.Sprintf("%.2fGbit", asGb)
	}
	if asMb > 1.0 {
		return fmt.Sprintf("%.2fMbit", asMb)
	}
	if asKb > 1.0 {
		return fmt.Sprintf("%.2fKbit", asKb)
	}
	return fmt.Sprintf("%vbit", bits)
}
