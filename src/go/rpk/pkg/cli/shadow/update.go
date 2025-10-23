// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"
	"reflect"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [LINK_NAME]",
		Short: "Update a Shadow Link",
		Long: `Update a Shadow Link

This command opens your default editor to modify the Shadow Link configuration.
Only the fields that are changed will be updated on the server.

The Shadow Link name cannot be changed. If you need to rename a Shadow Link,
you must delete and recreate it.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(prof)

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			linkName := args[0]
			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", err)

			shadowLink := link.Msg.GetShadowLink()
			originalCfg := shadowLinkToConfig(shadowLink)
			addRedactedPasswordString(originalCfg, shadowLink)

			// Open editor to modify the configuration.
			updatedCfg, err := rpkos.EditTmpYAMLFile(fs, originalCfg)
			out.MaybeDie(err, "unable to edit Shadow Link configuration: %v", err)

			if updatedCfg.Name != originalCfg.Name {
				out.Die("Shadow Link name cannot be changed; if you need to rename, please delete and recreate the shadow link")
			}

			if reflect.DeepEqual(originalCfg, updatedCfg) {
				out.Exit("No changes detected")
			}

			// No field mask used: we update all fields that are set in the
			// ShadowLink message.
			_, err = cl.ShadowLinkService().UpdateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.UpdateShadowLinkRequest{
				ShadowLink: shadowLinkConfigToProto(updatedCfg),
			}))
			out.MaybeDie(err, "unable to update Shadow Link: %v", err)

			fmt.Printf("Successfully updated shadow link %q.\n", linkName)
		},
	}
	return cmd
}

// if the password is set, replace it with a redacted value so user can provide
// a change easily instead of writing the full password field.
func addRedactedPasswordString(cfg *ShadowLinkConfig, link *adminv2.ShadowLink) {
	isPassSet := link.GetConfigurations().GetClientOptions().GetAuthenticationConfiguration().GetScramConfiguration().GetPasswordSet()
	if !isPassSet {
		return
	}
	if auth, ok := cfg.ClientOptions.AuthenticationConfiguration.(*ScramConfig); ok && auth != nil {
		auth.Password = "<redacted>"
	}
}
