// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
)

type logDirRow struct {
	Broker    int32  `json:"broker" yaml:"broker"`
	Dir       string `json:"dir" yaml:"dir"`
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
	Size      int64  `json:"size" yaml:"size"`
	Error     string `json:"error,omitempty" yaml:"error,omitempty"`
}

// collapseLogDirRows merges consecutive rows where shouldChange returns false,
// accumulating size into the prior row.
func collapseLogDirRows(rows []logDirRow, shouldChange func(prior, current logDirRow) bool) []logDirRow {
	if len(rows) == 0 {
		return rows
	}
	prior := rows[0]
	keep := rows[:0]
	for _, current := range rows[1:] {
		if shouldChange(prior, current) {
			keep = append(keep, prior)
			prior = current
			continue
		}
		prior.Size += current.Size
	}
	return append(keep, prior)
}

// aggregateAndSortLogDirs validates aggregateInto, collapses rows to the
// requested granularity, and optionally sorts by size descending.
func aggregateAndSortLogDirs(rows []logDirRow, aggregateInto string, sortBySize bool) ([]logDirRow, error) {
	switch strings.ToLower(aggregateInto) {
	default:
		return nil, fmt.Errorf("unrecognized --aggregate-into %q", aggregateInto)
	case "", "partition":
		// no collapse needed
	case "broker":
		rows = collapseLogDirRows(rows, func(prior, current logDirRow) bool {
			return prior.Broker != current.Broker
		})
	case "dir":
		rows = collapseLogDirRows(rows, func(prior, current logDirRow) bool {
			return prior.Broker != current.Broker || prior.Dir != current.Dir
		})
	case "topic":
		rows = collapseLogDirRows(rows, func(prior, current logDirRow) bool {
			return prior.Broker != current.Broker || prior.Dir != current.Dir || prior.Topic != current.Topic
		})
	}
	if sortBySize {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].Size >= rows[j].Size })
	}
	return rows, nil
}

func printLogDirs(f config.OutFormatter, rows []logDirRow, aggregateInto string, human bool, sortBySize bool, w io.Writer) {
	var err error
	rows, err = aggregateAndSortLogDirs(rows, aggregateInto, sortBySize)
	out.MaybeDie(err, "invalid --aggregate-into value")

	if isText, _, t, err := f.Format(rows); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}

	sizeFn := func(size int64) string {
		if human {
			return units.HumanSize(float64(size))
		}
		return strconv.Itoa(int(size))
	}

	var tw *out.TabWriter
	var printRow func(r logDirRow)
	switch strings.ToLower(aggregateInto) {
	default:
		// unreachable: validated in aggregateAndSortLogDirs
	case "", "partition":
		tw = out.NewTableTo(w, "BROKER", "DIR", "TOPIC", "PARTITION", "SIZE", "ERROR")
		printRow = func(r logDirRow) { tw.Print(r.Broker, r.Dir, r.Topic, r.Partition, sizeFn(r.Size), r.Error) }
	case "broker":
		tw = out.NewTableTo(w, "BROKER", "SIZE", "ERROR")
		printRow = func(r logDirRow) { tw.Print(r.Broker, sizeFn(r.Size), r.Error) }
	case "dir":
		tw = out.NewTableTo(w, "BROKER", "DIR", "SIZE", "ERROR")
		printRow = func(r logDirRow) { tw.Print(r.Broker, r.Dir, sizeFn(r.Size), r.Error) }
	case "topic":
		tw = out.NewTableTo(w, "BROKER", "DIR", "TOPIC", "SIZE", "ERROR")
		printRow = func(r logDirRow) { tw.Print(r.Broker, r.Dir, r.Topic, sizeFn(r.Size), r.Error) }
	}

	defer tw.Flush()
	for _, r := range rows {
		printRow(r)
	}
}

func newLogdirsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logdirs",
		Short: "Describe log directories on Redpanda brokers",
	}
	p.InstallKafkaFlags(cmd)
	cmd.AddCommand(
		newLogdirsDescribeCommand(fs, p),
	)
	return cmd
}

func newLogdirsDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		aggregateInto string
		broker        int32
		human         bool
		sortBySize    bool
		topics        []string
	)

	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe log directories on Redpanda brokers",
		Long: `Describe log directories on Redpanda brokers.

This command prints information about log directories on brokers, particularly,
the base directory that topics and partitions are located in, and the size of
data that has been written to the partitions. The size you see may not exactly
match the size on disk as reported by du: Redpanda allocates files in chunks.
The chunks will show up in du, while the actual bytes so far written to the
file will show up in this command.

The directory returned is the root directory for partitions. Within Redpanda,
the partition data lives underneath the the returned root directory in

    kafka/{topic}/{partition}_{revision}/

where revision is a Redpanda internal concept.
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]logDirRow{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var s kadm.TopicsSet
			if len(topics) > 0 {
				listed, err := adm.ListTopics(context.Background(), topics...)
				out.MaybeDie(err, "unable to describe topics: %v", err)
				var exit bool
				listed.EachError(func(d kadm.TopicDetail) {
					fmt.Fprintf(os.Stderr, "unable to discover the partitions on topic %q: %v\n", d.Topic, d.Err)
					exit = true
				})
				if exit {
					os.Exit(1)
				}
				s = listed.TopicsSet()
			}

			var rows []logDirRow

			eachDir := func(d kadm.DescribedLogDir) {
				if d.Err != nil {
					rows = append(rows, logDirRow{
						Broker: d.Broker,
						Dir:    d.Dir,
						Error:  d.Err.Error(),
					})
					return
				}
				d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
					rows = append(rows, logDirRow{
						Broker:    d.Broker,
						Dir:       d.Dir,
						Topic:     p.Topic,
						Partition: p.Partition,
						Size:      p.Size,
					})
				})
			}

			if broker >= 0 {
				desc, err := adm.DescribeBrokerLogDirs(context.Background(), broker, s)
				out.MaybeDie(err, "unable to describe broker log dirs: %v", err)
				desc.Each(eachDir)
			} else {
				desc, err := adm.DescribeAllLogDirs(context.Background(), s)
				out.HandleShardError("DescribeLogDirs", err)
				desc.Each(eachDir)
			}

			// Deeply sort rows first so aggregation can collapse consecutive equal keys.
			types.Sort(rows)

			printLogDirs(f, rows, aggregateInto, human, sortBySize, cmd.OutOrStdout())
		},
	}

	cmd.Flags().Int32VarP(&broker, "broker", "b", -1, "If non-negative, the specific broker to describe")
	cmd.Flags().BoolVar(&sortBySize, "sort-by-size", false, "If true, sort by size")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "Specific topics to describe")
	cmd.Flags().StringVar(&aggregateInto, "aggregate-into", "", "If non-empty, what column to aggregate into starting from the partition column (broker, dir, topic)")
	cmd.Flags().BoolVarP(&human, "human-readable", "H", false, "Print the logdirs size in a human-readable form")

	cmd.RegisterFlagCompletionFunc("aggregate-into", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		opts := []string{"broker", "dir", "topic"}
		return opts, cobra.ShellCompDirectiveDefault
	})
	p.InstallFormatFlag(cmd)
	return cmd
}
