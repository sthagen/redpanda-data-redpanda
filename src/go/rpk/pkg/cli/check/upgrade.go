// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package check

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func upgradeCommand(fs afero.Fs) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade to the latest Redpanda Check version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			pluginDir, err := plugin.DefaultBinPath()
			out.MaybeDie(err, "unable to determine managed plugin path: %v", err)
			check, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("check")
			if !pluginExists {
				out.Die("unable to find Redpanda Check plugin. You may install it running 'rpk check install'")
			}
			if !check.Managed {
				out.Die("found a self-managed Redpanda Check plugin; unfortunately, we cannot upgrade it with this installation. Run rpk check uninstall && rpk check install, or manage the binary manually")
			}
			art, version, err := getCheckArtifact(cmd.Context(), "latest")
			out.MaybeDieErr(err)

			currentSha, err := plugin.Sha256Path(fs, check.Path)
			out.MaybeDie(err, "unable to determine the sha256sum of current Redpanda Check %q: %v", check.Path, err)

			if strings.HasPrefix(currentSha, art.Sha256) {
				out.Exit("Redpanda Check already up-to-date")
			}
			currentVersion, err := checkVersion(cmd.Context(), check.Path)
			out.MaybeDie(err, "unable to determine current version of Redpanda Check: %v", err)

			if !noConfirm {
				latestVersion, err := redpanda.VersionFromString(version)
				out.MaybeDie(err, "unable to parse latest version of Redpanda Check: %v", err)
				if latestVersion.Major > currentVersion.Major {
					confirmed, err := out.Confirm("Confirm major version upgrade from %v to %v?", currentVersion.String(), latestVersion.String())
					out.MaybeDie(err, "unable to confirm upgrade: %v", err)
					if !confirmed {
						out.Exit("Upgrade canceled.")
					}
				}
			}

			_, err = downloadAndInstallCheck(cmd.Context(), fs, pluginDir, art.Path, art.Sha256)
			out.MaybeDieErr(err)

			fmt.Printf("Redpanda Check successfully upgraded from %v to the latest version (%v).\n", currentVersion.String(), version)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt for major version upgrades")
	return cmd
}

func checkVersion(ctx context.Context, checkPath string) (redpanda.Version, error) {
	versionCmd := exec.CommandContext(ctx, checkPath, "--version")
	var sb strings.Builder
	versionCmd.Stdout = &sb
	if err := versionCmd.Run(); err != nil {
		return redpanda.Version{}, err
	}
	// The redpanda-check binary outputs just the version string (e.g. "0.1.0")
	versionStr := strings.TrimSpace(sb.String())
	version, err := redpanda.VersionFromString(versionStr)
	if err != nil {
		return redpanda.Version{}, fmt.Errorf("unable to determine version from %q: %v", versionStr, err)
	}
	return version, nil
}
