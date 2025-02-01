// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package generate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	gatekeeperv1alpha1 "buf.build/gen/go/redpandadata/gatekeeper/protocolbuffers/go/redpanda/api/gatekeeper/v1alpha1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type licenseRequest struct {
	name     string
	lastname string
	company  string
	email    string
}

func newLicenseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		lr        licenseRequest
		path      string
		noConfirm bool
		apply     bool
	)
	cmd := &cobra.Command{
		Use:   "license",
		Short: "Generate a trial license",
		Long: `Generate a trial license

This command generates a license for a 30-day trial of Redpanda Enterprise 
Edition.

To get a permanent license, contact us: https://www.redpanda.com/contact

The license is saved in your working directory or the specified path, based 
on the --path flag.

To apply the license to your cluster, use the --apply flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			if lr.isEmpty() {
				err := lr.prompt()
				out.MaybeDieErr(err)
			}
			err = lr.validate()
			out.MaybeDieErr(err)

			req := connect.NewRequest(
				&gatekeeperv1alpha1.LicenseSignupRequest{
					GivenName:   lr.name,
					FamilyName:  lr.lastname,
					CompanyName: lr.company,
					Email:       lr.email,
					ClusterInfo: &gatekeeperv1alpha1.EnterpriseClusterInfo{
						ClusterId: "rpk-generated",
						Platform:  gatekeeperv1alpha1.EnterpriseClusterInfo_PLATFORM_REDPANDA,
					},
					RequestOrigin: gatekeeperv1alpha1.LicenseSignupRequest_REQUEST_ORIGIN_CLI,
				},
			)

			savePath, err := preparePath(fs, path, noConfirm)
			out.MaybeDieErr(err)

			cl := publicapi.NewEnterpriseClientSet(cfg.DevOverrides().PublicAPIURL)
			signup, err := cl.Gatekeeper.LicenseSignup(cmd.Context(), req)
			out.MaybeDie(err, "unable to request trial license: %v", err)

			licenseKey := signup.Msg.GetLicense().LicenseKey
			expirationDate := time.Now().Add(30 * 24 * time.Hour).Format(time.DateOnly)

			var errSave, errApply error
			errSave = rpkos.ReplaceFile(fs, savePath, []byte(licenseKey), 0o644)
			if apply {
				errApply = applyLicense(cmd.Context(), fs, cfg.VirtualProfile(), licenseKey, noConfirm)
			}

			var msg string
			switch {
			case errSave == nil && errApply == nil:
				msg = printLicense(apply, savePath, expirationDate)
			case errSave == nil && errApply != nil:
				msg = printSavedLicenseWithError(errApply, savePath, expirationDate)
			case errSave != nil:
				msg = printNotStoredLicense(errSave, errApply, apply, licenseKey, expirationDate)
			}
			fmt.Println(msg)
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "File path for generating the license")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt for overwriting and applying the generated license file")
	cmd.Flags().BoolVar(&apply, "apply", false, "Apply the generated license to your Redpanda cluster")
	// License request info.
	cmd.Flags().StringVar(&lr.name, "name", "", "First name for trial license registration")
	cmd.Flags().StringVar(&lr.lastname, "last-name", "", "Last name for trial license registration")
	cmd.Flags().StringVar(&lr.company, "company", "", "Company name for trial license registration")
	cmd.Flags().StringVar(&lr.email, "email", "", "Company email for trial license registration")

	cmd.MarkFlagsRequiredTogether("name", "last-name", "company", "email")
	return cmd
}

func applyLicense(ctx context.Context, fs afero.Fs, p *config.RpkProfile, licenseKey string, noConfirm bool) error {
	if len(p.AdminAPI.Addresses) == 0 {
		return fmt.Errorf("no Admin API addresses found in your configuration. Please provide them using an rpk profile or the corresponding flags")
	}
	if !noConfirm {
		tw := out.NewTable("broker")
		for _, address := range p.AdminAPI.Addresses {
			tw.Print(address)
		}
		tw.Flush()
		ok, err := out.Confirm("Apply license to the cluster above?")
		if err != nil {
			return fmt.Errorf("unable to confirm cluster: %v", err)
		}
		if !ok {
			return errors.New("apply canceled")
		}
	}
	cl, err := adminapi.NewClient(ctx, fs, p)
	if err != nil {
		return fmt.Errorf("unable to initialize admin client: %v", err)
	}
	err = cl.SetLicense(ctx, strings.NewReader(licenseKey))
	if err != nil {
		return fmt.Errorf("unable to set license key: %v", err)
	}
	return nil
}

func (l *licenseRequest) isEmpty() bool {
	return l.name == "" && l.lastname == "" && l.company == "" && l.email == ""
}

func (l *licenseRequest) prompt() error {
	name, err := out.Prompt("First Name:")
	if err != nil {
		return fmt.Errorf("unable to get the firt name: %v", err)
	}
	l.name = name
	lastname, err := out.Prompt("Last name:")
	if err != nil {
		return fmt.Errorf("unable to get the last name: %v", err)
	}
	l.lastname = lastname
	company, err := out.Prompt("Company:")
	if err != nil {
		return fmt.Errorf("unable to get the company name: %v", err)
	}
	l.company = company
	email, err := out.Prompt("Business Email:")
	if err != nil {
		return fmt.Errorf("unable to get the business email: %v", err)
	}
	l.email = email
	return nil
}

func (l *licenseRequest) validate() error {
	if l.name == "" {
		return errors.New("name cannot be empty")
	}
	if l.lastname == "" {
		return errors.New("lastname cannot be empty")
	}
	if l.email == "" {
		return errors.New("company email cannot be empty")
	}
	if l.company == "" {
		return errors.New("company name cannot be empty")
	}
	return nil
}

func preparePath(fs afero.Fs, path string, noConfirm bool) (string, error) {
	if path == "" {
		workingDir, err := os.Getwd()
		if err != nil {
			return "", err
		}
		path = filepath.Join(workingDir, "redpanda.license")
	} else {
		isDir, err := afero.IsDir(fs, path)
		if err != nil {
			return "", fmt.Errorf("unable to determine if path %q is a directory: %v", path, err)
		}
		if !isDir {
			return path, nil
		}
		path = filepath.Join(path, "redpanda.license")
	}
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return "", fmt.Errorf("unable to check if file %q exists: %v", path, err)
	}
	if exists && !noConfirm {
		confirm, err := out.Confirm("%q already exists. Do you want to overwrite it?", path)
		if err != nil {
			return "", errors.New("cancelled; unable to confirm license file overwrite; you may select a new saving path using the '--path' flag")
		}
		if !confirm {
			return "", fmt.Errorf("cancelled; overwrite not allowed on %q; you may select a new saving path using the '--path' flag", path)
		}
	}
	return path, nil
}

func printLicense(apply bool, savePath, expirationDate string) string {
	var builder strings.Builder

	if apply {
		builder.WriteString("\nThe license was successfully applied.\n")
	}

	builder.WriteString(fmt.Sprintf("\nSuccessfully saved license to %q.\n", savePath))

	if !apply {
		builder.WriteString(fmt.Sprintf(`
Upload this license in Redpanda Console, or run:
  rpk cluster license set --path %q
`, savePath))
	}

	builder.WriteString(fmt.Sprintf(`
This license expires on %s.

For more information, see:
  https://docs.redpanda.com/current/get-started/licensing/overview/#license-keys\
`, expirationDate))

	return builder.String()
}

func printSavedLicenseWithError(err error, savePath, expirationDate string) string {
	return fmt.Sprintf(`
Successfully saved license to %q.

Error applying the license to the cluster: %v

Upload this license in Redpanda Console, or run:
  rpk cluster license set --path %[1]v

This license expires on %[3]v.

For more information, see: 
  https://docs.redpanda.com/current/get-started/licensing/overview/#license-keys
`, savePath, err, expirationDate)
}

func printNotStoredLicense(errSave, errApply error, apply bool, license, expirationDate string) string {
	var sb strings.Builder
	sb.WriteString("\nSuccessfully generated a license.")
	if apply && errApply == nil {
		sb.WriteString(" The license has been uploaded to your cluster.")
	}

	// Add error details if any.
	if errSave != nil {
		sb.WriteString(fmt.Sprintf("\n\n- Error saving your license to file: %v", errSave))
	}
	if errApply != nil {
		sb.WriteString(fmt.Sprintf("\n- Error applying the license to the cluster: %v", errApply))
	}

	// Add license and expiration info.
	sb.WriteString(fmt.Sprintf(`

License: %v

WARNING: Save your license key. It cannot be accessed later.

This license expires on %v.
`, license, expirationDate))

	// Add apply instructions if needed.
	if !apply || errApply != nil {
		sb.WriteString(fmt.Sprintf(`
Upload this license in Redpanda Console, or run:
  rpk cluster license set %v
`, license))
	}

	sb.WriteString(`
For more information, see:
  https://docs.redpanda.com/current/get-started/licensing/overview/#license-keys`)

	return sb.String()
}
