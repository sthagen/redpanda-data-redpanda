// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package bundle

import (
	"fmt"
	"os"
	"time"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Use the same date specs as journalctl (see `man journalctl`).
const (
	timeHelpText = `(journalctl date format, e.g. YYYY-MM-DD)`
	outputFlag   = "output"
)

type bundleParams struct {
	fs                      afero.Fs
	cfg                     *config.Config
	cl                      *kgo.Client
	admin                   *admin.AdminAPI
	logsSince               string
	logsUntil               string
	path                    string
	logsLimitBytes          int
	controllerLogLimitBytes int
	timeout                 time.Duration
}

func NewCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		outFile    string

		brokers   []string
		user      string
		password  string
		mechanism string
		enableTLS bool
		certFile  string
		keyFile   string
		caFile    string

		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string

		logsSince     string
		logsUntil     string
		logsSizeLimit string

		controllerLogsSizeLimit string

		timeout time.Duration
	)
	command := &cobra.Command{
		Use:   "bundle",
		Short: "Collect environment data and create a bundle file for the Redpanda Data support team to inspect",
		Long:  bundleHelpText,
		Run: func(cmd *cobra.Command, args []string) {
			path, err := determineFilepath(fs, outFile, cmd.Flags().Changed(outputFlag))
			out.MaybeDie(err, "unable to determine filepath %q: %v", outFile, err)

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// We use NewHostClient because we want to talk to
			// localhost, and some of out API requests require
			// choosing the host to talk to. With params of
			// rpk.admin_api preferring localhost first IF no
			// rpk.admin_api is actually in the underlying file, we
			// can always just pick the first host.
			admin, err := admin.NewHostClient(fs, cfg, "0")
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			logsLimit, err := units.FromHumanSize(logsSizeLimit)
			out.MaybeDie(err, "unable to parse --logs-size-limit: %v", err)

			controllerLogsLimit, err := units.FromHumanSize(controllerLogsSizeLimit)
			out.MaybeDie(err, "unable to parse --controller-logs-size-limit: %v", err)
			bp := bundleParams{
				fs:                      fs,
				cfg:                     cfg,
				cl:                      cl,
				admin:                   admin,
				logsSince:               logsSince,
				logsUntil:               logsUntil,
				path:                    path,
				logsLimitBytes:          int(logsLimit),
				controllerLogLimitBytes: int(controllerLogsLimit),
				timeout:                 timeout,
			}

			// to execute the appropriate bundle we look for
			// kubernetes_service_* env variables as an indicator that we are
			// in a k8s environment
			host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
			if len(host) == 0 || len(port) == 0 {
				err = executeBundle(cmd.Context(), bp)
			} else {
				err = executeK8SBundle(cmd.Context(), bp)
			}
			out.MaybeDie(err, "unable to create bundle: %v", err)
		},
	}
	command.Flags().StringVar(
		&adminURL,
		config.FlagAdminHosts2,
		"",
		"Comma-separated list of admin API addresses (<IP>:<port>)",
	)
	command.Flags().StringVar(&adminURL, "admin-url", "", "")
	command.Flags().MarkDeprecated("admin-url", "use --"+config.FlagAdminHosts2)

	command.Flags().StringVarP(
		&outFile,
		outputFlag,
		"o",
		"",
		"The file path where the debug file will be written (default ./<timestamp>-bundle.zip)",
	)
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10*time.Second,
		"How long to wait for child commands to execute (e.g. '30s', '1.5m')",
	)
	command.Flags().StringVar(
		&logsSince,
		"logs-since",
		"",
		fmt.Sprintf(`Include log entries on or newer than the specified date. %s`, timeHelpText),
	)
	command.Flags().StringVar(
		&logsUntil,
		"logs-until",
		"",
		fmt.Sprintf(`Include log entries on or older than the specified date. %s`, timeHelpText),
	)
	command.Flags().StringVar(
		&logsSizeLimit,
		"logs-size-limit",
		"100MiB",
		"Read the logs until the given size is reached. Multipliers are also supported, e.g. 3MB, 1GiB",
	)
	command.Flags().StringVar(
		&controllerLogsSizeLimit,
		"controller-logs-size-limit",
		"20MB",
		"Sets the limit of the controller log size that can be stored in the bundle. Multipliers are also supported, e.g. 3MB, 1GiB",
	)

	common.AddKafkaFlags(
		command,
		&configFile,
		&user,
		&password,
		&mechanism,
		&enableTLS,
		&certFile,
		&keyFile,
		&caFile,
		&brokers,
	)
	common.AddAdminAPITLSFlags(command,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	return command
}

const bundleHelpText = `'rpk debug bundle' collects environment data that can help debug and diagnose
issues with a redpanda cluster, a broker, or the machine it's running on. It
then bundles the collected data into a zip file.

The following are the data sources that are bundled in the compressed file:

 - Kafka metadata: Broker configs, topic configs, start/committed/end offsets,
   groups, group commits.

 - Controller logs: The controller logs directory up to a limit set by
   --controller-logs-size-limit flag

 - Data directory structure: A file describing the data directory's contents.

 - redpanda configuration: The redpanda configuration file (redpanda.yaml;
   SASL credentials are stripped).

 - /proc/cpuinfo: CPU information like make, core count, cache, frequency.

 - /proc/interrupts: IRQ distribution across CPU cores.

 - Resource usage data: CPU usage percentage, free memory available for the
   redpanda process.

 - Clock drift: The ntp clock delta (using pool.ntp.org as a reference) & round
   trip time.

 - Kernel logs: The kernel logs ring buffer (syslog).

 - Broker metrics: The local broker's Prometheus metrics, fetched through its
   admin API.

 - DNS: The DNS info as reported by 'dig', using the hosts in
   /etc/resolv.conf.

 - Disk usage: The disk usage for the data directory, as output by 'du'.

 - redpanda logs: The redpanda logs written to journald. If --logs-since or
   --logs-until are passed, then only the logs within the resulting time frame
   will be included.

 - Socket info: The active sockets data output by 'ss'.

 - Running process info: As reported by 'top'.

 - Virtual memory stats: As reported by 'vmstat'.

 - Network config: As reported by 'ip addr'.

 - lspci: List the PCI buses and the devices connected to them.

 - dmidecode: The DMI table contents. Only included if this command is run
   as root.
`
