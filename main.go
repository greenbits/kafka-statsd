package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"github.com/segmentio/go-log"
	"gopkg.in/alexcesaro/statsd.v2"
)

var (
	brokers      = kingpin.Flag("broker", "A kafka broker to connect to.  Specify multiple times for multiple brokers. (e.g. host1:9092)").HintOptions("host1:9092").Short('b').Envar("KSTATSD_BROKERS").Required().Strings()
	statsdAddr   = kingpin.Flag("statsd-addr", "Statsd address").Short('s').Default("127.0.0.1").Envar("KSTATSD_STATSD_ADDR").String()
	statsdPort   = kingpin.Flag("statsd-port", "Statsd port").Short('P').Default("8125").Envar("KSTATSD_STATSD_PORT").String()
	statsdPrefix = kingpin.Flag("statsd-prefix", "Statsd prefix").Short('p').Envar("KSTATSD_STATSD_PREFIX").String()
	interval     = kingpin.Flag("refresh-interval", "Interval to refresh offset lag in seconds").Short('i').Default("5").Envar("KSTATSD_INTERVAL").Int()
	tagType      = kingpin.Flag("tag-format", "Format to use when encoding tags (Options: none, influxdb, datadog)").HintOptions(statsdTagOptionsEnum()...).Default("none").Envar("KSTATSD_USE_TAGS").Enum(statsdTagOptionsEnum()...)
	includeTags  = kingpin.Flag("tag", "Tags to include.  Specify multiple times for multiple tags. (e.g. tagname:value)").HintOptions("tagname:value").Envar("KSTATSD_TAGS").Strings()
)

var statsdTagFormat = map[string]statsd.TagFormat{
	"influxdb": statsd.InfluxDB,
	"datadog":  statsd.Datadog,
	"none":     0,
}

func statsdTagOptionsEnum() []string {
	res := make([]string, 0, len(statsdTagFormat))
	for k := range statsdTagFormat {
		res = append(res, k)
	}
	return res
}

func newStatsdClient() (*statsd.Client, error) {
	tags := make([]string, 0, len(*includeTags)*2)
	for _, tag := range *includeTags {
		splitTag := strings.SplitN(tag, ":", 2)
		tags = append(tags, splitTag...)
	}

	opts := []statsd.Option{
		statsd.Address(strings.Join([]string{*statsdAddr, *statsdPort}, ":")),
		statsd.ErrorHandler(func(err error) {
			log.Error("Statsd error: %s", err)
		}),
	}

	if *statsdPrefix != "" {
		opts = append(opts, statsd.Prefix(*statsdPrefix))
	}

	if len(tags) > 0 {
		opts = append(opts, statsd.Tags(tags...))
	}

	tagFormat := statsdTagFormat[*tagType]
	if tagFormat != 0 {
		opts = append(opts, statsd.TagsFormat(tagFormat))
	}

	return statsd.New(opts...)
}

func isTaggedReporting() bool {
	// There's probably a better way to do this than string comparison
	return *tagType != "none"
}

func main() {
	kingpin.Parse()

	statsdClient, err := newStatsdClient()
	if err != nil {
		log.Error("Error creating statsd client: %s", err)
		return
	}
	defer statsdClient.Close()

	client, err := sarama.NewClient(*brokers, nil)
	if err != nil {
		log.Error("Error connecting to Kafka (client): %s", err)
		return
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Error("Error closing kafka client connection: %s", err)
		}
	}()

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(*brokers, config)
	if err != nil {
		log.Error("Error connecting to Kafka (admin): %s", err)
		return
	}
	defer func() {
		err := admin.Close()
		if err != nil {
			log.Error("Error closing kafka admin connection: %s", err)
		}
	}()

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Info("Starting consumer offset daemon")

	for {
		select {
		case <-ticker.C:
			log.Info("Refetching consumer offset lag")
			err := refreshAndReportMetrics(statsdClient, client, admin)
			if err != nil {
				log.Error("Error occurred while refreshing cluster lag: %s", err)
			}

		case <-signals:
			log.Info("Got interrupt signal, exiting.")
			return
		}
	}
}

func refreshAndReportMetrics(statsdClient *statsd.Client, client sarama.Client, admin sarama.ClusterAdmin) error {
	// No need to thread statsd client interaction, the statsd client does buffered/batch sending for us

	clusterState, err := collectClusterState(client, admin)
	if err != nil {
		return errors.Wrap(err, "getting consumer groups")
	}

	for topic, parts := range clusterState.TopicOffsets {
		log.Debug("Reporting offsets for topic %s (parts: %v)", topic, parts)
		for partition, position := range parts {
			stats := statsdClient.Clone(
				statsd.Tags("topic", topic),
				statsd.Tags("partition", strconv.FormatInt(int64(partition), 10)),
			)
			if isTaggedReporting() {
				stats.Gauge("partition.offset", position)
			} else {
				key := fmt.Sprintf("topic.%s.partition.%d.offset", topic, partition)
				stats.Gauge(key, position)
			}
		}
	}

	for group, topicMap := range clusterState.ConsumerOffsets {
		log.Debug("Reporting offsets for consumer group %s (parts :%v)", group, topicMap)

		for topic, partitionMap := range topicMap {
			for partition, consumerOffset := range partitionMap {
				stats := statsdClient.Clone(
					statsd.Tags("topic", topic),
					statsd.Tags("partition", strconv.FormatInt(int64(partition), 10)),
					statsd.Tags("consumer_group", group),
				)

				topicOffset := clusterState.TopicOffsets[topic][partition]
				lag := topicOffset - consumerOffset

				if isTaggedReporting() {
					stats.Gauge("consumer.offset", consumerOffset)
					stats.Gauge("consumer.lag", lag)
				} else {
					offsetKey := fmt.Sprintf("topic.%s.partition.%d.consumer_group.%s.offset", topic, partition, group)
					stats.Gauge(offsetKey, consumerOffset)

					lagKey := fmt.Sprint("topic.%s.partition.%d.consumer_group.%s.offset", topic, partition, group)
					stats.Gauge(lagKey, lag)
				}
			}
		}
	}

	return nil
}

func getConsumerGroups(admin sarama.ClusterAdmin) ([]string, error) {
	cgMap, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, errors.Wrap(err, "ListConsumerGroups")
	}
	consumerGroups := make([]string, 0, len(cgMap))
	for k := range cgMap {
		// special kafka topic that tracks consumer offsets
		if k != "__consumer_offsets" {
			consumerGroups = append(consumerGroups, k)
		}
	}
	return consumerGroups, nil
}

func getConsumerGroupOffsets(admin sarama.ClusterAdmin, groups ...string) (map[string]map[string]map[int32]int64, error) {
	type res struct {
		group string
		value *sarama.OffsetFetchResponse
		err   error
	}
	responses := make(chan *res, len(groups))
	var wg sync.WaitGroup

	for _, group := range groups {
		wg.Add(1)
		go func(grp string) {
			defer wg.Done()
			response, err := admin.ListConsumerGroupOffsets(grp, nil)
			responses <- &res{
				group: grp,
				value: response,
				err:   err,
			}
		}(group)
	}

	wg.Wait()
	close(responses)

	// Map consumer group -> topic -> partition -> comnsumer offset
	result := make(map[string]map[string]map[int32]int64, len(groups))
	for r := range responses {
		if r.err != nil {
			return nil, r.err
		}

		topicMap, ok := result[r.group]
		if !ok {
			topicMap = make(map[string]map[int32]int64, len(r.value.Blocks))
			result[r.group] = topicMap
		}

		for topic, block := range r.value.Blocks {
			partitionMap := make(map[int32]int64, len(block))
			topicMap[topic] = partitionMap
			for partitionId, offsetBlock := range block {
				partitionMap[partitionId] = offsetBlock.Offset
			}
		}
	}

	return result, nil
}

func getOffsetsFromTopicAndPartitions(client sarama.Client, topic string, partitions []int32) (map[int32]int64, error) {
	result := map[int32]int64{}

	type response struct {
		partition int32
		offset    int64
		err       error
	}

	ch := make(chan *response, len(partitions))
	var wg sync.WaitGroup

	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			ch <- &response{
				partition: partition,
				offset:    offset,
				err:       err,
			}
		}(partition)
	}

	wg.Wait()
	close(ch)

	var lastErr error
	for res := range ch {
		if res.err != nil {
			lastErr = res.err
		} else {
			result[res.partition] = res.offset
		}
	}

	if lastErr != nil {
		return nil, errors.Wrap(lastErr, "GetOffset")
	}

	return result, nil
}

func getTopicPartitions(client sarama.Client) (map[string]map[int32]int64, error) {
	topics, err := client.Topics()
	if err != nil {
		return nil, errors.Wrap(err, "Topics")
	}

	type response struct {
		topic string
		pts   map[int32]int64
		err   error
	}
	ch := make(chan *response, len(topics))
	var wg sync.WaitGroup

	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			pts, err := client.Partitions(topic)
			if err != nil {
				ch <- &response{
					topic: topic,
					err:   errors.Wrap(err, "Partitions"),
				}
				return
			}

			parts, err := getOffsetsFromTopicAndPartitions(client, topic, pts)
			if err != nil {
				err = errors.Wrap(err, "getOffsetsFromTopicAndPartitions")
			}
			ch <- &response{
				topic: topic,
				pts:   parts,
				err:   err,
			}
		}(topic)
	}

	wg.Wait()
	close(ch)

	partitions := make(map[string]map[int32]int64, len(topics))
	for res := range ch {
		if res.err != nil {
			return nil, res.err
		}
		partitions[res.topic] = res.pts
	}
	return partitions, nil
}

// ClusterState is a snapshot recording of the current kafka cluster's state
type ClusterState struct {
	// Mapping of consumer groups to topics to partitions to consumer offsets
	ConsumerOffsets map[string]map[string]map[int32]int64

	// Map of topics to partitions to latest partition offset
	TopicOffsets map[string]map[int32]int64
}

// NewClusterState gathers the current state about consumers and topics in the cluster
func collectClusterState(client sarama.Client, admin sarama.ClusterAdmin) (*ClusterState, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	cs := &ClusterState{}
	var err error
	go func() {
		defer wg.Done()
		var cgs []string
		var e error
		cgs, e = getConsumerGroups(admin)
		if e != nil {
			err = errors.Wrap(e, "getConsumerGroups")
			return
		}
		cs.ConsumerOffsets, e = getConsumerGroupOffsets(admin, cgs...)
		if e != nil {
			err = errors.Wrap(e, "getConsumerGroupOffsets")
		}
	}()

	go func() {
		defer wg.Done()
		var e error
		cs.TopicOffsets, e = getTopicPartitions(client)
		if e != nil {
			err = errors.Wrap(e, "getTopicPartitions")
		}
	}()
	wg.Wait()

	if err != nil {
		return nil, errors.Wrap(err, "NewClusterState")
	}

	return cs, nil
}
