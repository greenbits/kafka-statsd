package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/quipo/statsd"
	"github.com/segmentio/go-log"

	"github.com/alecthomas/kingpin"
)

var (
	brokers      = kingpin.Flag("brokers", "Comma separated list of kafka brokers (e.g. host1:9092,host2:9092").Short('b').Envar("KSTATSD_BROKERS").Required().String()
	statsdAddr   = kingpin.Flag("statsd-addr", "Statsd address").Short('s').Default("127.0.0.1:8125").Envar("KSTATSD_STATSD_ADDR").String()
	statsdPrefix = kingpin.Flag("statsd-prefix", "Statsd prefix").Short('p').Envar("KSTATSD_STATSD_PREFIX").String()
	interval     = kingpin.Flag("refresh-interval", "Interval to refresh offset lag in seconds").Short('i').Default("5").Envar("KSTATSD_INTERVAL").Int()
	useTags      = kingpin.Flag("use-tags", "Use tags if your StatsD client supports them (like DataDog and InfluxDB)").Default("false").Envar("KSTATSD_USE_TAGS").Bool()
	includeTags  = kingpin.Flag("include-tags", "Tags to include, if you want to include a host name or datacenter for example.").Envar("KSTATSD_TAGS").Strings()
)

type ClusterState struct {
	// List of consumer groups that exist in the cluster
	ConsumerGroups []string

	// Map of topics to topic partitions
	Topics map[string][]int32
}

func main() {
	kingpin.Parse()

	statsdClient := statsd.NewStatsdClient(*statsdAddr, *statsdPrefix)
	err := statsdClient.CreateSocket()
	if err != nil {
		log.Error("Error creating statsd client: %s", err)
		return
	}
	stats := statsd.NewStatsdBuffer(time.Second, statsdClient)
	defer stats.Close()

	brokerList := strings.Split(*brokers, ",")

	client, err := sarama.NewClient(brokerList, nil)
	if err != nil {
		log.Error("Error connecting to Kafka (client): %s", err)
		return
	}
	defer client.Close()

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Error("Error connecting to Kafka (admin): %s", err)
		return
	}
	defer admin.Close()

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Info("Starting consumer offset daemon")

	for {
		select {
		case <-ticker.C:
			log.Info("Refetching consumer offset lag")

			// map of "consumer group name" -> "the word `consumer`"
			clusterState, err := NewClusterState(client, admin)
			if err != nil {
				log.Error("Error getting consumer groups: %s", err)
				return
			}

			topicPartitions, err := getTopicPartitions(client)
			if err != nil {
				log.Error("getTopicPartitions: %s", err)
				return
			}

			for _, cg := range clusterState.ConsumerGroups {
				log.Debug("Getting offsets for consumer group: %s", cg)

				oldOffsets, err := getOffsetsFromConsumerGroup(admin, cg, topicPartitions)
				if err != nil {
					log.Error("getOffsetsFromConsumerGroup: %s", err)
					return
				}

				for topic, partitionOffsets := range oldOffsets {
					log.Debug("Getting offsets for topic: %s", topic)

					latestOffsets, err := getOffsetsFromTopicAndPartitions(client, topic, topicPartitions[topic])
					if err != nil {
						log.Error("getOffsetsFromTopicAndPartitions: %s", err)
						return
					}

					for partitionID, offset := range latestOffsets {
						log.Debug("Sending lag for partition ID: %d", partitionID)

						lag := offset - partitionOffsets[partitionID]

						if *useTags {
							var tags []string
							tags = append(tags, "topic="+topic)
							tags = append(tags, fmt.Sprintf("partition=%d", partitionID))
							tags = append(tags, "consumer_group="+cg)
							if includeTags != nil {
								for _, t := range *includeTags {
									tags = append(tags, t)
								}
							}
							stats.Gauge(fmt.Sprintf("consumer_lag,%s", strings.Join(tags, ",")), lag)
						} else {
							stats.Gauge(fmt.Sprintf("topic.%s.partition.%d.consumer_group.%s.lag", topic, partitionID, cg), lag)
						}
					}
				}
			}

		case <-signals:
			log.Info("Got interrupt signal, exiting.")
			return
		}
	}
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

func getOffsetsFromConsumerGroup(admin sarama.ClusterAdmin, group string, topicPartitions map[string][]int32) (map[string]map[int32]int64, error) {
	resp, err := admin.ListConsumerGroupOffsets(group, topicPartitions)
	if err != nil {
		return nil, errors.Wrap(err, "ListConsumerGroupOffsets")
	}

	result := map[string]map[int32]int64{}

	for topic, block := range resp.Blocks {
		if topic != "__consumer_offsets" {
			topicMap := map[int32]int64{}
			result[topic] = topicMap
			for partitionID, offsetBlock := range block {
				topicMap[partitionID] = offsetBlock.Offset
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
	ch := make(chan *response)
	defer close(ch)

	for _, partition := range partitions {
		go func(partition int32) {
			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			ch <- &response{
				partition: partition,
				offset:    offset,
				err:       err,
			}
		}(partition)
	}

	var lastErr error
	for i := 0; i < len(partitions); i++ {
		res := <-ch
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

func getTopicPartitions(client sarama.Client) (map[string][]int32, error) {
	topics, err := client.Topics()
	if err != nil {
		return nil, errors.Wrap(err, "Topics")
	}

	type response struct {
		topic string
		pts   []int32
		err   error
	}
	ch := make(chan *response)

	for _, topic := range topics {
		go func(topic string) {
			pts, err := client.Partitions(topic)
			ch <- &response{
				topic: topic,
				pts:   pts,
				err:   err,
			}
		}(topic)
	}

	partitions := map[string][]int32{}
	var lastErr error
	for i := 0; i < len(topics); i++ {
		res := <-ch
		if res.err != nil {
			lastErr = res.err
		} else {
			partitions[res.topic] = res.pts
		}
	}

	if lastErr != nil {
		return nil, errors.Wrap(lastErr, "Partitions")
	}
	return partitions, nil
}

// NewClusterState gathers the current state about consumers and topics in the cluster
func NewClusterState(client sarama.Client, admin sarama.ClusterAdmin) (*ClusterState, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	cs := &ClusterState{}
	var err error
	go func() {
		defer wg.Done()
		cs.ConsumerGroups, err = getConsumerGroups(admin)
	}()
	go func() {
		defer wg.Done()
		cs.Topics, err = getTopicPartitions(client)
	}()
	wg.Wait()

	if err != nil {
		return nil, errors.Wrap(err, "NewClusterState")
	}

	return cs, nil
}
