// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/docker/go-units"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

func newMetadataCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		cluster         bool
		brokers         bool
		topics          bool
		internal        bool
		detailedTopics  bool
		detailedBrokers bool
		includeDecom    bool
	)
	cmd := &cobra.Command{
		Use:     "info",
		Aliases: []string{"status", "metadata"},
		Short:   "Request broker metadata",
		Long: `Request broker metadata information.

The Kafka protocol's metadata contains information about brokers, topics, and
the cluster as a whole.

This command only runs if specific sections of metadata are requested. There
are currently three sections: the cluster, the list of brokers, and the topics.
If no section is specified, this defaults to printing all sections.

If the topic section is requested, all topics are requested by default unless
some are manually specified as arguments. Expanded per-partition information
can be printed with the -d flag, and internal topics can be printed with the -i
flag.

In the broker section, the controller node is suffixed with *.

Using --detailed augments the broker section with Admin API metadata: the
number of cores per broker, membership status, liveness, version, and broker
UUID (when reported by the Admin API). It also prints a per-broker DISK SPACE
section.

Using --include-decommissioned additionally lists decommissioned nodes by
their UUID at the end of the broker table.

If the Admin API is unreachable when --detailed or --include-decommissioned
is set, the basic broker view is still printed; pass -v to see the underlying
error.

Using this command with --format json/yaml implies that all sections are
included.
`,
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]metadataResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			// We first evaluate whether any section was requested.
			// If none were, we default to all sections. Only after
			// do we evaluate whether detailed was requested, which
			// implies topics (and must come after defaulting all
			// sections).
			requestedSections := 0
			for _, v := range []*bool{&cluster, &brokers, &topics} {
				if *v {
					requestedSections++
				}
			}
			if len(args) > 0 || detailedTopics || internal {
				topics = true
			}
			if requestedSections == 0 || !f.IsText() { // default to all sections
				cluster, brokers, topics, internal = true, true, true, true
				requestedSections = 4
			}

			// If the user requested more than one section, we
			// print a header for the section.
			header := func(name string, fn func()) {
				if requestedSections > 1 {
					fmt.Println(name)
					fmt.Println(strings.Repeat("=", len(name)))
					defer fmt.Println()
				}
				fn()
			}

			var m kadm.Metadata
			if topics || len(args) > 0 {
				m, err = adm.Metadata(context.Background(), args...)
			} else {
				m, err = adm.BrokerMetadata(context.Background())
			}
			out.MaybeDie(err, "unable to request metadata: %v", err)

			var ainfo *adminBrokerInfo
			if (detailedBrokers || includeDecom) && brokers {
				ainfo = fetchAdminBrokerInfo(cmd.Context(), fs, p)
			}

			if !f.IsText() {
				err = printRawMetadataToW(cmd.OutOrStdout(), f, m, ainfo, detailedBrokers, includeDecom)
				out.MaybeDie(err, "unable to print metadata in %q format: %v", f.Kind, err)
				return
			}
			// We only print the cluster section if the response
			// has a cluster.
			if cluster && m.Cluster != "" {
				header("CLUSTER", func() {
					fmt.Printf("%s\n", m.Cluster)
				})
			}
			if brokers {
				detailedSections := requestedSections
				if detailedBrokers && ainfo != nil && hasAnyDiskSpace(ainfo) {
					detailedSections++
				}
				bh := func(name string, fn func()) {
					if detailedSections > 1 {
						fmt.Println(name)
						fmt.Println(strings.Repeat("=", len(name)))
						defer fmt.Println()
					}
					fn()
				}
				bh("BROKERS", func() {
					printBrokers(cmd.OutOrStdout(), m.Controller, m.Brokers, ainfo, includeDecom)
				})
				if detailedBrokers && ainfo != nil && hasAnyDiskSpace(ainfo) {
					bh("DISK SPACE", func() {
						printDiskSpace(cmd.OutOrStdout(), ainfo)
					})
				}
			}
			if topics && len(m.Topics) > 0 {
				header("TOPICS", func() {
					PrintTopics(m.Topics, internal, detailedTopics)
				})
			}
		},
	}
	p.InstallKafkaFlags(cmd)
	p.InstallAdminFlags(cmd)
	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&cluster, "print-cluster", "c", false, "Print cluster section")
	cmd.Flags().BoolVarP(&brokers, "print-brokers", "b", false, "Print brokers section")
	cmd.Flags().BoolVarP(&topics, "print-topics", "t", false, "Print topics section (implied if any topics are specified)")
	cmd.Flags().BoolVarP(&internal, "print-internal-topics", "i", false, "Print internal topics (if all topics requested, implies -t)")
	cmd.Flags().BoolVarP(&detailedTopics, "print-detailed-topics", "d", false, "Print per-partition information for topics (implies -t)")
	cmd.Flags().BoolVar(&detailedBrokers, "detailed", false, "Augment the brokers section with Admin API metadata (cores, membership, liveness, version, UUID, and disk space)")
	cmd.Flags().BoolVar(&includeDecom, "include-decommissioned", false, "Append decommissioned nodes to the brokers section using their UUID")
	return cmd
}

// adminBrokerInfo aggregates the Admin API metadata that supplements the
// Kafka metadata when --detailed or --include-decommissioned is set.
type adminBrokerInfo struct {
	byNodeID   map[int32]rpadmin.Broker
	uuidByNode map[int32]string      // joined string when a node has multiple UUIDs
	decom      []rpadmin.BrokerUuids // UUIDs that don't appear in byNodeID
}

// fetchAdminBrokerInfo queries the Admin API for per-broker metadata, broker
// UUIDs, and decommissioned nodes. Returns nil when the Admin API is
// unreachable; callers fall back to the basic broker view. Failures are
// logged via zap so the broker section still renders for users without
// admin access.
func fetchAdminBrokerInfo(ctx context.Context, fs afero.Fs, p *config.RpkProfile) *adminBrokerInfo {
	log := zap.L().Sugar()
	cl, err := adminapi.NewClient(ctx, fs, p)
	if err != nil {
		log.Errorf("unable to initialize admin client: %v", err)
		return nil
	}
	bs, err := cl.Brokers(ctx)
	if err != nil {
		log.Errorf("unable to fetch brokers via admin api: %v", err)
		return nil
	}
	info := &adminBrokerInfo{byNodeID: make(map[int32]rpadmin.Broker, len(bs))}
	for _, b := range bs {
		info.byNodeID[int32(b.NodeID)] = b
	}

	// Broker UUIDs are optional: clusters that don't expose them should not
	// fail the whole call.
	uuids, err := cl.GetBrokerUuids(ctx)
	if err != nil {
		log.Errorf("unable to retrieve node UUIDs: %v", err)
	}
	if len(uuids) == 0 {
		return info
	}
	uuidLists := make(map[int32][]string, len(uuids))
	for _, u := range uuids {
		uuidLists[int32(u.NodeID)] = append(uuidLists[int32(u.NodeID)], u.UUID)
	}
	info.uuidByNode = make(map[int32]string, len(uuidLists))
	for id, list := range uuidLists {
		info.uuidByNode[id] = strings.Join(list, ", ")
	}
	for _, u := range uuids {
		if _, ok := info.byNodeID[int32(u.NodeID)]; !ok {
			info.decom = append(info.decom, u)
		}
	}
	slices.SortFunc(info.decom, func(a, b rpadmin.BrokerUuids) int { return cmp.Compare(a.NodeID, b.NodeID) })
	return info
}

func hasAnyDiskSpace(info *adminBrokerInfo) bool {
	for _, b := range info.byNodeID {
		if len(b.DiskSpace) > 0 {
			return true
		}
	}
	return false
}

func printBrokers(w io.Writer, controllerID int32, brokers kadm.BrokerDetails, info *adminBrokerInfo, includeDecom bool) {
	headers := []string{"ID", "HOST", "PORT"}
	args := func(b *kadm.BrokerDetail) []any {
		ret := []any{b.NodeID, b.Host, b.Port}
		if b.NodeID == controllerID {
			ret[0] = fmt.Sprintf("%d*", b.NodeID)
		}
		return ret
	}

	// Rack is included if any broker has a rack.
	for i := range brokers {
		if brokers[i].Rack != nil {
			headers = append(headers, "RACK")
			orig := args
			args = func(b *kadm.BrokerDetail) []any {
				var rack string
				if b.Rack != nil {
					rack = *b.Rack
				}
				return append(orig(b), rack)
			}

			break
		}
	}

	if info != nil {
		headers = append(headers, "CORES", "MEMBERSHIP", "IS-ALIVE", "VERSION")
		orig := args
		args = func(b *kadm.BrokerDetail) []any {
			ai, ok := info.byNodeID[b.NodeID]
			if !ok {
				return append(orig(b), "-", "-", "-", "-")
			}
			version, _ := redpanda.VersionFromString(ai.Version)
			alive := "-"
			if ai.IsAlive != nil {
				alive = fmt.Sprintf("%t", *ai.IsAlive)
			}
			return append(orig(b), ai.NumCores, string(ai.MembershipStatus), alive, version.String())
		}
	}
	hasUUIDs := info != nil && info.uuidByNode != nil
	if hasUUIDs {
		headers = append(headers, "UUID")
		orig := args
		args = func(b *kadm.BrokerDetail) []any {
			uuid, ok := info.uuidByNode[b.NodeID]
			if !ok {
				uuid = "-"
			}
			return append(orig(b), uuid)
		}
	}

	tw := out.NewTableTo(w, headers...)
	defer tw.Flush()
	for _, broker := range brokers {
		tw.Print(args(&broker)...)
	}
	if includeDecom && info != nil {
		// Decommissioned nodes only have a NodeID and a UUID; the rest of
		// the columns are filled with "-" so the row aligns with the table.
		filler := make([]any, len(headers))
		for i := range filler {
			filler[i] = "-"
		}
		uuidIdx := -1
		if hasUUIDs {
			uuidIdx = len(headers) - 1
		}
		for _, d := range info.decom {
			row := append([]any(nil), filler...)
			row[0] = d.NodeID
			if uuidIdx >= 0 {
				row[uuidIdx] = d.UUID
			}
			tw.Print(row...)
		}
	}
}

func printDiskSpace(w io.Writer, info *adminBrokerInfo) {
	tw := out.NewTableTo(w, "NODE-ID", "PATH", "FREE", "TOTAL", "USED%")
	defer tw.Flush()
	ids := make([]int32, 0, len(info.byNodeID))
	for id := range info.byNodeID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		b := info.byNodeID[id]
		for _, d := range b.DiskSpace {
			var usedPct float64
			if d.Total > 0 {
				usedPct = float64(d.Total-d.Free) / float64(d.Total) * 100
			}
			tw.Print(b.NodeID, d.Path, units.HumanSize(float64(d.Free)), units.HumanSize(float64(d.Total)), fmt.Sprintf("%.1f%%", usedPct))
		}
	}
}

// RunBrokerInfo runs the equivalent of `rpk cluster info -b --detailed
// --include-decommissioned` against the given profile and writes the result
// to w. It exists to back the deprecated `rpk redpanda admin brokers list`
// shim so the legacy implementation can be removed.
func RunBrokerInfo(ctx context.Context, fs afero.Fs, prof *config.RpkProfile, f config.OutFormatter, w io.Writer, includeDecom bool) error {
	adm, err := kafka.NewAdmin(fs, prof)
	if err != nil {
		return fmt.Errorf("unable to initialize kafka client: %v", err)
	}
	defer adm.Close()

	m, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return fmt.Errorf("unable to request metadata: %v", err)
	}

	info := fetchAdminBrokerInfo(ctx, fs, prof)

	if !f.IsText() {
		return printRawMetadataToW(w, f, m, info, true, includeDecom)
	}

	const (
		secBrokers   = "BROKERS"
		secDiskSpace = "DISK SPACE"
	)
	showDisk := info != nil && hasAnyDiskSpace(info)
	section := func(name string, fn func()) {
		if showDisk {
			fmt.Fprintln(w, name)
			fmt.Fprintln(w, strings.Repeat("=", len(name)))
			defer fmt.Fprintln(w)
		}
		fn()
	}
	section(secBrokers, func() {
		printBrokers(w, m.Controller, m.Brokers, info, includeDecom)
	})
	if showDisk {
		section(secDiskSpace, func() {
			printDiskSpace(w, info)
		})
	}
	return nil
}

func PrintTopics(topics kadm.TopicDetails, internal, detailed bool) {
	if !detailed {
		tw := out.NewTable("NAME", "PARTITIONS", "REPLICAS")
		defer tw.Flush()

		for _, topic := range topics.Sorted() {
			if !internal && topic.IsInternal {
				continue
			}
			parts := len(topic.Partitions)
			replicas := topic.Partitions.NumReplicas()
			tw.Print(topic.Topic, parts, replicas)
		}
		return
	}

	buf := new(bytes.Buffer)
	buf.Grow(512)
	defer func() { os.Stdout.Write(buf.Bytes()) }()

	for i, topic := range topics.Sorted() {
		if topic.IsInternal && !internal {
			continue
		}
		if i > 0 {
			fmt.Fprintln(buf)
		}

		// "foo, 20 partitions, 3 replicas"
		fmt.Fprintf(buf, "%s", topic.Topic)
		if topic.IsInternal {
			fmt.Fprint(buf, " (internal)")
		}
		fmt.Fprintf(buf, ", %d partitions", len(topic.Partitions))
		if len(topic.Partitions) > 0 {
			fmt.Fprintf(buf, ", %d replicas", len(topic.Partitions[0].Replicas))
		}
		buf.WriteString("\n")

		// We include certain columns if any partition has a
		// non-default value.
		var useEpoch, useOffline, useErr bool
		for _, p := range topic.Partitions.Sorted() {
			if p.LeaderEpoch != -1 {
				useEpoch = true
			}
			if len(p.OfflineReplicas) > 0 {
				useOffline = true
			}
			if p.Err != nil {
				useErr = true
			}
		}

		// Since this is a nested table, we use one leading empty
		// header, which tabs the entire table in one. We also use an
		// empty leading column in our args below.
		headers := []string{"", "partition", "leader"}
		if useEpoch {
			headers = append(headers, "epoch")
		}
		headers = append(headers, "replicas") // TODO add isr see #1928
		if useOffline {
			headers = append(headers, "offline-replicas")
		}
		if useErr {
			headers = append(headers, "load-error")
		}

		args := func(p *kadm.PartitionDetail) []any {
			ret := []any{"", p.Partition, p.Leader}
			if useEpoch {
				ret = append(ret, p.LeaderEpoch)
			}
			ret = append(ret, int32s(p.Replicas).sort())
			if useOffline {
				ret = append(ret, int32s(p.OfflineReplicas).sort())
			}
			if useErr {
				if p.Err != nil {
					ret = append(ret, p.Err.Error())
				} else {
					ret = append(ret, "-")
				}
			}
			return ret
		}

		tw := out.NewTableTo(buf, headers...)
		for _, part := range topic.Partitions.Sorted() {
			tw.Print(args(&part)...)
		}
		tw.Flush()
	}
}

type metadataResponse struct {
	ClusterName  string         `json:"cluster_name" yaml:"cluster_name"`
	ControllerID int            `json:"controller_id" yaml:"controller_id"`
	Brokers      []BrokerDetail `json:"brokers" yaml:"brokers"`
	Topics       []TopicDetail  `json:"topics" yaml:"topics"`
}

type BrokerDetail struct {
	ID         int32       `json:"id" yaml:"id"`
	Host       string      `json:"host,omitempty" yaml:"host,omitempty"`
	Port       int32       `json:"port,omitempty" yaml:"port,omitempty"`
	Rack       *string     `json:"rack,omitempty" yaml:"rack,omitempty"`
	Cores      *int        `json:"cores,omitempty" yaml:"cores,omitempty"`
	Membership string      `json:"membership,omitempty" yaml:"membership,omitempty"`
	IsAlive    *bool       `json:"is_alive,omitempty" yaml:"is_alive,omitempty"`
	Version    string      `json:"version,omitempty" yaml:"version,omitempty"`
	UUID       string      `json:"uuid,omitempty" yaml:"uuid,omitempty"`
	DiskSpace  []DiskSpace `json:"disk_space,omitempty" yaml:"disk_space,omitempty"`
}

type DiskSpace struct {
	Path        string  `json:"path" yaml:"path"`
	Free        int64   `json:"free" yaml:"free"`
	Total       int64   `json:"total" yaml:"total"`
	UsedPercent float64 `json:"used_percent" yaml:"used_percent"`
}

type TopicDetail struct {
	Name           string            `json:"name" yaml:"name"`
	IsInternal     bool              `json:"is_internal" yaml:"is_internal"`
	PartitionCount int               `json:"partition_count" yaml:"partition_count"`
	ReplicasCount  int               `json:"replicas_count" yaml:"replicas_count"`
	Partitions     []PartitionDetail `json:"partitions" yaml:"partitions"`
}

type PartitionDetail struct {
	Partition       int32   `json:"partition" yaml:"partition"`
	Leader          int32   `json:"leader" yaml:"leader"`
	LeaderEpoch     int32   `json:"leader_epoch" yaml:"leader_epoch"`
	Replicas        []int32 `json:"replicas" yaml:"replicas"`
	OfflineReplicas []int32 `json:"offline_replicas,omitempty" yaml:"offline_replicas,omitempty"`
	Error           string  `json:"error,omitempty" yaml:"error,omitempty"`
}

func printRawMetadataToW(w io.Writer, f config.OutFormatter, m kadm.Metadata, info *adminBrokerInfo, detailed, includeDecom bool) error {
	resp := metadataResponse{
		ClusterName:  m.Cluster,
		ControllerID: int(m.Controller),
	}
	var brokers []BrokerDetail
	for _, b := range m.Brokers {
		bd := BrokerDetail{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
			Rack: b.Rack,
		}
		if info != nil {
			if ai, ok := info.byNodeID[b.NodeID]; ok {
				if detailed {
					cores := ai.NumCores
					bd.Cores = &cores
					bd.Membership = string(ai.MembershipStatus)
					bd.IsAlive = ai.IsAlive
					version, _ := redpanda.VersionFromString(ai.Version)
					bd.Version = version.String()
					for _, d := range ai.DiskSpace {
						var usedPct float64
						if d.Total > 0 {
							usedPct = float64(d.Total-d.Free) / float64(d.Total) * 100
						}
						bd.DiskSpace = append(bd.DiskSpace, DiskSpace{
							Path:        d.Path,
							Free:        int64(d.Free),
							Total:       int64(d.Total),
							UsedPercent: usedPct,
						})
					}
				}
			}
			if uuid, ok := info.uuidByNode[b.NodeID]; ok {
				bd.UUID = uuid
			}
		}
		brokers = append(brokers, bd)
	}
	if includeDecom && info != nil {
		for _, d := range info.decom {
			brokers = append(brokers, BrokerDetail{ID: int32(d.NodeID), UUID: d.UUID})
		}
	}
	resp.Brokers = brokers
	var topics []TopicDetail
	if len(m.Topics) > 0 {
		for _, topic := range m.Topics.Sorted() {
			td := TopicDetail{
				Name:           topic.Topic,
				IsInternal:     topic.IsInternal,
				PartitionCount: len(topic.Partitions),
			}
			if len(topic.Partitions) > 0 {
				td.ReplicasCount = len(topic.Partitions[0].Replicas)
			}
			var partitions []PartitionDetail
			for _, p := range topic.Partitions.Sorted() {
				pd := PartitionDetail{
					Partition:       p.Partition,
					Leader:          p.Leader,
					LeaderEpoch:     p.LeaderEpoch,
					Replicas:        p.Replicas,
					OfflineReplicas: p.OfflineReplicas,
				}
				if p.Err != nil {
					pd.Error = p.Err.Error()
				}
				partitions = append(partitions, pd)
			}
			td.Partitions = partitions
			topics = append(topics, td)
		}
	}
	resp.Topics = topics
	_, _, t, err := f.Format(&resp)
	if err != nil {
		return fmt.Errorf("unable to print in the requested format %q: %v", f.Kind, err)
	}
	_, err = fmt.Fprintln(w, t)
	if err != nil {
		return err
	}
	return nil
}

type int32s []int32

func (is int32s) sort() []int32 {
	slices.Sort(is)
	return is
}
