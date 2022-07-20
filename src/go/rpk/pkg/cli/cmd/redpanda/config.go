// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"errors"
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	vnet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	configFileFlag     = "config"
	configFileFlagDesc = "Redpanda config file, if not set the file will be searched for in the default location"
)

func NewConfigCommand(fs afero.Fs) *cobra.Command {
	root := &cobra.Command{
		Use:   "config <command>",
		Short: "Edit configuration",
	}
	root.AddCommand(set(fs))
	root.AddCommand(bootstrap(fs))
	root.AddCommand(initNode(fs))

	return root
}

func set(fs afero.Fs) *cobra.Command {
	var (
		format     string
		configPath string
	)
	c := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set configuration values, such as the redpanda node ID or the list of seed servers",
		Long: `Set configuration values, such as the redpanda node ID or the list of seed servers

This command modifies the redpanda.yaml you have locally on disk. The first
argument is the key within the yaml representing a property / field that you
would like to set. Nested fields can be accessed through a dot:

  rpk redpanda config set redpanda.developer_mode true

The default format is to parse the value as yaml. Individual specific fields
can be set, or full structs:

  rpk redpanda config set rpk.tune_disk_irq true
  rpk redpanda config set redpanda.rpc_server '{address: 3.250.158.1, port: 9092}'

You can set an entire array by wrapping all items with braces, or by using one
struct:

  rpk redpanda config set redpanda.advertised_kafka_api '[{address: 0.0.0.0, port: 9092}]'
  rpk redpanda config set redpanda.advertised_kafka_api '{address: 0.0.0.0, port: 9092}' # same

Indexing can be used to set specific items in an array. You can index one past
the end of an array to extend it:

  rpk redpanda config set redpanda.advertised_kafka_api[1] '{address: 0.0.0.0, port: 9092}'

The json format can be used to set values as json:

  rpk redpanda config set redpanda.rpc_server '{"address":"0.0.0.0","port":33145}' --format json

`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			cfg = cfg.FileOrDefaults() // we set fields in the raw file without writing env / flag overrides

			if format == "single" {
				fmt.Println("'--format single' is deprecated, either remove it or use yaml/json")
			}
			err = cfg.Set(args[0], args[1], format)
			out.MaybeDie(err, "unable to set %q:%v", args[0], err)

			err = cfg.Write(fs)
			out.MaybeDieErr(err)
		},
	}
	c.Flags().StringVar(&format, "format", "yaml", "Format of the value (yaml/json)")
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		configFileFlagDesc,
	)
	return c
}

func bootstrap(fs afero.Fs) *cobra.Command {
	var (
		ips        []string
		self       string
		id         int
		configPath string
	)
	c := &cobra.Command{
		Use:   "bootstrap --id <id> [--self <ip>] [--ips <ip1,ip2,...>]",
		Short: "Initialize the configuration to bootstrap a cluster",
		Long:  helpBootstrap,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			cfg = cfg.FileOrDefaults() // we modify fields in the raw file without writing env / flag overrides

			seeds, err := parseSeedIPs(ips)
			out.MaybeDieErr(err)

			ownIP, err := parseSelfIP(self)
			out.MaybeDieErr(err)

			cfg.Redpanda.ID = id
			cfg.Redpanda.RPCServer.Address = ownIP.String()
			cfg.Redpanda.KafkaAPI = []config.NamedSocketAddress{{
				Address: ownIP.String(),
				Port:    config.DefaultKafkaPort,
			}}

			cfg.Redpanda.AdminAPI = []config.NamedSocketAddress{{
				Address: ownIP.String(),
				Port:    config.DefaultAdminPort,
			}}
			cfg.Redpanda.SeedServers = []config.SeedServer{}
			cfg.Redpanda.SeedServers = seeds

			err = cfg.Write(fs)
			out.MaybeDie(err, "error writing config file: %v", err)
		},
	}
	c.Flags().StringSliceVar(
		&ips,
		"ips",
		[]string{},
		"The list of known node addresses or hostnames",
	)
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		configFileFlagDesc,
	)
	c.Flags().StringVar(
		&self,
		"self",
		"",
		"Hint at this node's IP address from within the list passed in --ips",
	)
	c.Flags().IntVar(
		&id,
		"id",
		-1,
		"This node's ID (required).",
	)
	cobra.MarkFlagRequired(c.Flags(), "id")
	return c
}

func initNode(fs afero.Fs) *cobra.Command {
	var configPath string
	c := &cobra.Command{
		Use:   "init",
		Short: "Init the node after install, by setting the node's UUID",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			cfg = cfg.FileOrDefaults() // we modify fields in the raw file without writing env / flag overrides

			// Don't reset the node's UUID if it has already been set.
			if cfg.NodeUUID == "" {
				id, err := uuid.NewUUID()
				out.MaybeDie(err, "error creating nodeUUID: %v", err)
				cfg.NodeUUID = id.String()
			}

			err = cfg.Write(fs)
			out.MaybeDie(err, "error writing config file: %v", err)
		},
	}
	c.Flags().StringVar(
		&configPath,
		configFileFlag,
		"",
		configFileFlagDesc,
	)
	return c
}

func parseSelfIP(self string) (net.IP, error) {
	if self != "" {
		ownIP := net.ParseIP(self)
		if ownIP == nil {
			return nil, fmt.Errorf("%s is not a valid IP", self)
		}
		return ownIP, nil
	} else {
		ownIP, err := getOwnIP()
		if err != nil {
			return nil, err
		}
		return ownIP, nil
	}
}

func parseSeedIPs(ips []string) ([]config.SeedServer, error) {
	defaultRPCPort := config.Default().Redpanda.RPCServer.Port
	var seeds []config.SeedServer

	for _, i := range ips {
		_, hostport, err := vnet.ParseHostMaybeScheme(i)
		if err != nil {
			return nil, err
		}

		host, port := vnet.SplitHostPortDefault(hostport, defaultRPCPort)
		seed := config.SeedServer{
			Host: config.SocketAddress{
				Address: host,
				Port:    port,
			},
		}
		seeds = append(seeds, seed)
	}
	return seeds, nil
}

func getOwnIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	filtered := []net.IP{}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		isV4 := ipnet.IP.To4() != nil
		private, err := isPrivate(ipnet.IP)
		if err != nil {
			return nil, err
		}

		if isV4 && private && !ipnet.IP.IsLoopback() {
			filtered = append(filtered, ipnet.IP)
		}
	}
	if len(filtered) > 1 {
		return nil, errors.New(
			"found multiple private non-loopback v4 IPs for the" +
				" current node. Please set one with --self",
		)
	}
	if len(filtered) == 0 {
		return nil, errors.New(
			"couldn't find any non-loopback IPs for the current node",
		)
	}
	return filtered[0], nil
}

func isPrivate(ip net.IP) (bool, error) {
	// The standard private subnet CIDRS
	privateCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
	}
	for _, cidr := range privateCIDRs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return false, err
		}
		if ipNet.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}

const helpBootstrap = `Initialize the configuration to bootstrap a cluster.

--id is mandatory. bootstrap will expect the machine it's running on
to have only one private non-loopback IP address associated to it,
and use it in the configuration as the node's address.

If it has multiple IPs, --self must be specified.
In that case, the given IP will be used without checking whether it's
among the machine's addresses or not.

The elements in --ips must be separated by a comma, no spaces.

If omitted, the node will be configured as a root node, that other
ones can join later.
`
