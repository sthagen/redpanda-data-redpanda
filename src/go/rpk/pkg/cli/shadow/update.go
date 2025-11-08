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
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [LINK_NAME]",
		Short: "Update a Shadow Link",
		Long: `Update a Shadow Link.

This command opens your default editor with the current Shadow Link
configuration. Update the fields you want to change, save the file, and close
the editor. The command applies only the changed fields to the Shadow Link.

You cannot change the Shadow Link name. If you need to rename a Shadow Link,
delete it and create a new one with the desired name.

The editor respects your EDITOR environment variable. If EDITOR is not set, the
command uses 'vi' on Unix-like systems.
`,
		Example: `
Update a Shadow Link configuration:
  rpk shadow update my-shadow-link
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
			out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", handleConnectError(err, "get", linkName))

			shadowLink := link.Msg.GetShadowLink()
			originalCfg := shadowLinkToConfig(shadowLink)
			addRedactedPasswordString(originalCfg, shadowLink)

			// Open editor to modify the configuration.
			updatedCfg, err := rpkos.EditTmpYAMLFile(fs, originalCfg)
			out.MaybeDie(err, "unable to edit Shadow Link configuration: %v", err)

			err = validateParsedShadowLinkConfig(updatedCfg)
			out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

			if updatedCfg.Name != originalCfg.Name {
				out.Die("Shadow Link name cannot be changed; if you need to rename, please delete and recreate the shadow link")
			}

			diff := diffConfigs(originalCfg, updatedCfg)
			if diff == nil {
				out.Exit("No changes detected")
			}
			updatedSL := shadowLinkConfigToProto(updatedCfg)
			fm, err := fieldmaskpb.New(updatedSL, diff...)
			// This should not be possible, as diff is generated from valid
			// fields, but better to catch the errors here than submit an update
			// that the user didn't ask for.
			out.MaybeDie(err, "unrecognized changed fields: %v; please report this with Redpanda Support", err)

			zap.L().Sugar().Debugf("Requesting configuration update for: %v", strings.Join(diff, ", "))
			_, err = cl.ShadowLinkService().UpdateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.UpdateShadowLinkRequest{
				ShadowLink: updatedSL,
				UpdateMask: fm,
			}))
			out.MaybeDie(err, "unable to update Shadow Link: %v", handleConnectError(err, "update", linkName))

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
	if auth := cfg.ClientOptions.AuthenticationConfiguration; auth != nil && auth.ScramConfiguration != nil {
		auth.ScramConfiguration.Password = "<redacted>"
	}
}

// diffConfigs compares two ShadowLinkConfig objects and returns a list of
// fields that changed. It uses Reflect to compare the fields and returns the
// full JSON path of the changed fields, starting with "configurations".
//
// For example, if the 'bootstrap_servers' field changed, it returns
// "configurations.client_options.bootstrap_servers".
func diffConfigs(original, updated *ShadowLinkConfig) []string {
	if reflect.DeepEqual(original, updated) {
		return nil // deeply equal, no changes.
	}
	if original == nil || updated == nil {
		// One is nil, other is not: everything changed.
		return []string{"configurations"}
	}

	var changedPaths []string
	compareValues(reflect.ValueOf(original), reflect.ValueOf(updated), "configurations", &changedPaths)
	return changedPaths
}

// getJSONTag extracts the JSON tag name from a struct field.
// Returns empty string if the field should be skipped.
func getJSONTag(field reflect.StructField) string {
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return ""
	}
	// Extract the name before any options ("name,omitempty" -> "name")
	parts := strings.Split(jsonTag, ",")
	return parts[0]
}

// compareValues recursively compares two reflect.Value objects and records
// changed paths.
func compareValues(original, updated reflect.Value, path string, changedPaths *[]string) {
	if !original.IsValid() && !updated.IsValid() {
		return
	}
	// One invalid, other valid: record as changed
	if !original.IsValid() || !updated.IsValid() {
		*changedPaths = append(*changedPaths, path)
		return
	}

	// Dereference pointers
	if original.Kind() == reflect.Ptr {
		if original.IsNil() && updated.IsNil() {
			return
		}
		if original.IsNil() || updated.IsNil() {
			*changedPaths = append(*changedPaths, path)
			return
		}
		compareValues(original.Elem(), updated.Elem(), path, changedPaths)
		return
	}

	switch original.Kind() {
	case reflect.Struct:
		compareStructs(original, updated, path, changedPaths)
	default:
		// For other types, just use DeepEqual.
		if !reflect.DeepEqual(original.Interface(), updated.Interface()) {
			*changedPaths = append(*changedPaths, path)
		}
	}
}

// compareStructs compares two struct values field by field.
func compareStructs(original, updated reflect.Value, path string, changedPaths *[]string) {
	typ := original.Type()

	// Handle empty structs (markers like StartAtEarliest)
	if typ.NumField() == 0 {
		// Empty struct - just check existence (already handled by pointer nil check)
		return
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		jsonTag := getJSONTag(field)
		if jsonTag == "" {
			continue
		}
		fieldPath := path + "." + jsonTag

		compareValues(original.Field(i), updated.Field(i), fieldPath, changedPaths)
	}
}
