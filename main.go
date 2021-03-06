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
	"gopkg.in/alexcesaro/statsd.v2"

	log "github.com/sirupsen/logrus"
)

var (
	brokers      = kingpin.Flag("broker", "A kafka broker to connect to.  Specify multiple times for multiple brokers. (e.g. host1:9092)").HintOptions("host1:9092").Short('b').Envar("KSTATSD_BROKERS").Required().Strings()
	statsdAddr   = kingpin.Flag("statsd-addr", "Statsd address").Short('s').Default("127.0.0.1").Envar("KSTATSD_STATSD_ADDR").String()
	statsdPort   = kingpin.Flag("statsd-port", "Statsd port").Short('P').Default("8125").Envar("KSTATSD_STATSD_PORT").String()
	statsdPrefix = kingpin.Flag("statsd-prefix", "Statsd prefix").Short('p').Envar("KSTATSD_STATSD_PREFIX").String()
	interval     = kingpin.Flag("refresh-interval", "Interval to refresh offset lag in seconds").Short('i').Default("5").Envar("KSTATSD_INTERVAL").Int()
	tagType      = kingpin.Flag("tag-format", "Format to use when encoding tags (Options: none, influxdb, datadog)").HintOptions(statsdTagOptionsEnum()...).Default("none").Envar("KSTATSD_USE_TAGS").Enum(statsdTagOptionsEnum()...)
	includeTags  = kingpin.Flag("tag", "Tags to include.  Specify multiple times for multiple tags. (e.g. tagname:value)").HintOptions("tagname:value").Envar("KSTATSD_TAGS").Strings()
	logFormat    = kingpin.Flag("log-format", "Output format for logs").HintOptions("text", "json").Default("text").Envar("KSTATSD_LOG_FORMAT").Enum("text", "json")
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
			log.WithField("error", err).Error("Statsd Error")
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

	// Set up logging
	log.SetLevel(log.TraceLevel)
	switch *logFormat {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	case "text":
		log.SetFormatter(&log.TextFormatter{})
	}

	statsdClient, err := newStatsdClient()
	if err != nil {
		log.WithField("error", err).Error("Could not create statsd client")
		return
	}
	defer statsdClient.Close()

	client, err := sarama.NewClient(*brokers, nil)
	if err != nil {
		log.WithField("error", err).Error("Error creating kafka client connection")
		return
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.WithField("error", err).Error("Error closing kafka client connection")
		}
	}()

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	admin, err := sarama.NewClusterAdmin(*brokers, config)
	if err != nil {
		log.WithField("error", err).Error("Error creating kafka admin connection")
		return
	}
	defer func() {
		err := admin.Close()
		if err != nil {
			log.WithField("error", err).Error("Error closing kafka admin connection")
		}
	}()

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.WithFields(log.Fields{
		"interval": *interval,
		"brokers": brokers,
	}).Info("Starting consumer offset daemon")

	for {
		select {
		case <-ticker.C:
			log.Info("Refetching consumer offset lag")
			err := refreshAndReportMetrics(statsdClient, client, admin)
			if err != nil {
				log.WithField("error", err).Error("Error refreshingg consumer lag")
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
		log.WithFields(log.Fields{
			"topic": topic,
			"parts": parts,
		}).Debug("Reporting offsets for topic")
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
		log.WithFields(log.Fields{
			"consumer_group": group,
			"topic_map": topicMap,
		}).Debug("Reporting offsets for consumer group")

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

					lagKey := fmt.Sprintf("topic.%s.partition.%d.consumer_group.%s.offset", topic, partition, group)
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

func getConsumerGroupOffsets(admin sarama.ClusterAdmin, partitions map[string][]int32, groups []string) (map[string]map[string]map[int32]int64, error) {
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
			response, err := admin.ListConsumerGroupOffsets(grp, partitions)
			log.WithFields(log.Fields{
				"group": grp,
				"response": response,
				"err": err,
			}).Trace("admin.ListConsumerGroups(group, nil) = response, err")
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
			for partitionID, offsetBlock := range block {
				partitionMap[partitionID] = offsetBlock.Offset
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

func getTopicPartitionOffsets(client sarama.Client, topics map[string][]int32) (map[string]map[int32]int64, error) {
	type response struct {
		topic string
		pts   map[int32]int64
		err   error
	}
	ch := make(chan *response, len(topics))
	var wg sync.WaitGroup
	wg.Add(len(topics))

	for topic, partitions := range topics {
		go func(topic string, partitions []int32) {
			defer wg.Done()
			parts, err := getOffsetsFromTopicAndPartitions(client, topic, partitions)
			ch <- &response{
				topic: topic,
				pts:   parts,
				err:   err,
			}
		}(topic, partitions)
	}

	wg.Wait()
	close(ch)

	result := make(map[string]map[int32]int64, len(topics))
	for res := range ch {
		if res.err != nil {
			return nil, res.err
		}
		result[res.topic] = res.pts
	}
	return result, nil
}

func getTopicPartitions(client sarama.Client, topics []string) (map[string][]int32, error) {
	type response struct {
		topic string
		pts   []int32
		err   error
	}
	ch := make(chan *response, len(topics))
	var wg sync.WaitGroup
	wg.Add(len(topics))

	for _, topic := range topics {
		go func(topic string) {
			defer wg.Done()
			pts, err := client.Partitions(topic)
			ch <- &response{
				topic: topic,
				pts:   pts,
				err:   err,
			}
		}(topic)
	}

	wg.Wait()
	close(ch)

	partitions := make(map[string][]int32, len(topics))
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
	topics, err := client.Topics()
	if err != nil {
		return nil, errors.Wrap(err, "Topics")
	}

	partitions, err := getTopicPartitions(client, topics)
	if err != nil {
		return nil, errors.Wrap(err, "getTopicPartitions")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	cs := &ClusterState{}
	go func() {
		defer wg.Done()
		var cgs []string
		var e error
		cgs, e = getConsumerGroups(admin)
		if e != nil {
			err = errors.Wrap(e, "getConsumerGroups")
			return
		}
		cs.ConsumerOffsets, e = getConsumerGroupOffsets(admin, partitions, cgs)
		if e != nil {
			err = errors.Wrap(e, "getConsumerGroupOffsets")
		}
	}()

	go func() {
		defer wg.Done()
		var e error
		cs.TopicOffsets, e = getTopicPartitionOffsets(client, partitions)
		if e != nil {
			err = errors.Wrap(e, "getTopicPartitionOffsets")
		}
	}()
	wg.Wait()

	if err != nil {
		return nil, errors.Wrap(err, "NewClusterState")
	}

	return cs, nil
}
