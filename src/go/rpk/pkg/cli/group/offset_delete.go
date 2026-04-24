// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type offsetDeleteResult struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
	Status    string `json:"status" yaml:"status"`
}

func buildOffsetDeleteResults(responses kadm.DeleteOffsetsResponses) ([]offsetDeleteResult, bool) {
	ok := true
	var results []offsetDeleteResult

	topics := make([]string, 0, len(responses))
	for topic := range responses {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		partitionErrors := responses[topic]
		partitions := make([]int32, 0, len(partitionErrors))
		for p := range partitionErrors {
			partitions = append(partitions, p)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

		for _, partition := range partitions {
			msg := "OK"
			if e := partitionErrors[partition]; e != nil {
				ok = false
				msg = e.Error()
			}
			results = append(results, offsetDeleteResult{Topic: topic, Partition: partition, Status: msg})
		}
	}
	return results, ok
}

func printOffsetDeleteResults(f config.OutFormatter, results []offsetDeleteResult, w io.Writer) {
	if isText, _, t, err := f.Format(results); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := out.NewTabWriterTo(w)
	defer tw.Flush()
	for _, r := range results {
		tw.Print(r.Topic, r.Partition, r.Status)
	}
}

func NewOffsetDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		fromFile        string
		topicPartitions []string
	)
	cmd := &cobra.Command{
		Use:   "offset-delete [GROUP] --from-file FILE --topic foo:0,1,2",
		Short: "Delete offsets for a kafka group",
		Long: `Forcefully delete offsets for a kafka group.

The broker will only allow the request to succeed if the group is in a dead
state (no subscriptions) or there are no subscriptions for offsets for
topic/partitions requested to be deleted.

Use either the --from-file or the --topic option. They are mutually exclusive.
To indicate which topics or topic partitions you'd like to remove offsets from use
the --topic (-t) flag, followed by a comma separated list of partition ids. Supplying
no list will delete all offsets for all partitions for a given topic.

You may also provide a text file to indicate topic/partition tuples. Use the
--from-file flag for this option. The file must contain lines of topic/partitions
separated by a tab or space. Example:

topic_a 0
topic_a 1
topic_b 0
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]offsetDeleteResult{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			// Parse input from file or command line
			var topicsSet kadm.TopicsSet
			if len(topicPartitions) > 0 {
				topicsSet, err = parseTopicPartitionsArgs(topicPartitions)
			} else if fromFile != "" {
				topicsSet, err = parseTopicPartitionsFile(fs, fromFile)
			} else {
				err = fmt.Errorf("must pass at least one command line argument --topic or --from-file")
			}
			out.MaybeDieErr(err)

			// For -topic options that didn't include partitions, perform a
			// lookup for them using a metadata request
			if empty := topicsSet.EmptyTopics(); len(empty) > 0 {
				mdResp, err := adm.Metadata(context.Background(), empty...)
				out.MaybeDie(err, "unable to make metadata request: %v", err)
				topicsSet.Merge(mdResp.Topics.TopicsSet())
			}

			responses, err := adm.DeleteOffsets(context.Background(), args[0], topicsSet)
			out.MaybeDieErr(err)

			results, ok := buildOffsetDeleteResults(responses)
			printOffsetDeleteResults(f, results, cmd.OutOrStdout())
			if !ok { // At least one row contained an error.
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringVarP(&fromFile, "from-file", "f", "", "File of topic/partition tuples for which to delete offsets for")
	cmd.Flags().StringArrayVarP(&topicPartitions, "topic", "t", nil, "topic:partition_id (repeatable; e.g. -t foo:0,1,2 )")
	cmd.MarkFlagsMutuallyExclusive("from-file", "topic")
	p.InstallFormatFlag(cmd)
	return cmd
}

func parseTopicPartitionsArgs(list []string) (kadm.TopicsSet, error) {
	parsed, err := out.ParseTopicPartitions(list)
	if err == nil {
		topicsList := make(kadm.TopicsList, 0, len(parsed))
		for topic, partitions := range parsed {
			topicsList = append(topicsList, kadm.TopicPartitions{Topic: topic, Partitions: partitions})
		}
		return topicsList.IntoSet(), nil
	}
	return nil, err
}

func parseTopicPartitionsFile(fs afero.Fs, file string) (kadm.TopicsSet, error) {
	parsed, err := out.ParseFileArray[struct {
		Topic     string
		Partition int32
	}](fs, file)
	if err == nil {
		topicsSet := make(kadm.TopicsSet)
		for _, element := range parsed {
			topicsSet.Add(element.Topic, element.Partition)
		}
		return topicsSet, nil
	}
	return nil, err
}
