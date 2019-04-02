package main

import (
	// "fmt"
	// "os"
	// "os/signal"
	"strings"
	"sync"

	// "time"
	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
	// "github.com/quipo/statsd"
	"github.com/segmentio/go-log"

	// "github.com/wvanbergen/kazoo-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokers      = kingpin.Flag("brokers", "Kafka addresses (e.g. host1:9092,host2:9092)").Short('b').String()
	statsdAddr   = kingpin.Flag("statsd-addr", "Statsd address").Short('s').String()
	statsdPrefix = kingpin.Flag("statsd-prefix", "Statsd prefix").Short('p').String()
	interval     = kingpin.Flag("refresh-interval", "Interval to refresh offset lag in seconds").Short('i').Default("5").Int()
	useTags      = kingpin.Flag("use-tags", "Use tags if your StatsD client supports them (like DataDog and InfluxDB)").Default("false").Bool()
	includeTags  = kingpin.Flag("include-tags", "Tags to include, if you want to include a host name or datacenter for example.").Strings()
)

type ClusterState struct {
	// List of consumer groups that exist in the cluster
	ConsumerGroups []string

	// Map of topics to topic partitions
	Topics map[string][]int32
}

func main() {
	kingpin.Parse()

	// statsdClient := statsd.NewStatsdClient(*statsdAddr, *statsdPrefix)
	// err := statsdClient.CreateSocket()
	// if err != nil {
	// 	log.Error("Error creating statsd client: %s", err)
	// 	return
	// }
	// stats := statsd.NewStatsdBuffer(time.Second, statsdClient)
	// defer stats.Close()

	client, err := sarama.NewClient(strings.Split(*brokers, ","), nil)
	if err != nil {
		log.Error("Error connecting to Kafka (client): %s", err)
		return
	}
	defer client.Close()

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(strings.Split(*brokers, ","), config)
	if err != nil {
		log.Error("Error connecting to Kafka (admin): %s", err)
		return
	}
	defer admin.Close()

	// map of "consumer group name" -> "the word `consumer`"
	clusterState, err := NewClusterState(client, admin)
	if err != nil {
		log.Error("Error getting consumer groups: %s", err)
		return
	}
	log.Info("consumer groups %V", clusterState)

	// consumerGroupList, err := client.ListConsumerGroups()
	// if err != nil {
	// 	log.Error("%s", err)
	// 	return
	// }

	// ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	// signals := make(chan os.Signal, 1)
	// signal.Notify(signals, os.Interrupt)

	// for {
	// 	select {
	// 	case <-ticker.C:
	// 		log.Info("Refreshing offset lag")
	// 		err = gatherStats(client, stats)

	// 		for _, cg := range consumerGroupList {
	// 			offsets, err := cg.FetchAllOffsets()
	// 			if err != nil {
	// 				log.Error("%s", err)
	// 				return
	// 			}
	// 			for topic, m := range offsets {
	// 				for partitionID, cgOffset := range m {
	// 					tOffset, err := client.GetOffset(topic, partitionID, sarama.OffsetNewest)
	// 					if err != nil {
	// 						log.Error("%s", err)
	// 						return
	// 					}
	// 					lag := tOffset - cgOffset

	// 					log.Info("Topic: %s, Partition: %d, Consumer Group: %s, Lag: %d", topic, partitionID, cg.Name, lag)
	// 					if *useTags {
	// 						var tags []string
	// 						tags = append(tags, "topic="+topic)
	// 						tags = append(tags, fmt.Sprintf("partition=%d", partitionID))
	// 						tags = append(tags, "consumer_group="+cg.Name)
	// 						if includeTags != nil {
	// 							for _, t := range *includeTags {
	// 								tags = append(tags, t)
	// 							}
	// 						}
	// 						stats.Gauge(fmt.Sprintf("consumer_lag,%s", strings.Join(tags, ",")), lag)
	// 					} else {
	// 						stats.Gauge(fmt.Sprintf("topic.%s.partition.%d.consumer_group.%s.lag", topic, partitionID, cg.Name), lag)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	case <-signals:
	// 		log.Info("Got interrupt signal, exiting.")
	// 		return
	// 	}
	// }
}

// func gatherStats(kafka sarama.ClusterAdmin, stats *statsd.StatsdBuffer) error {
// 	consumerGroups, err := kafka.ListConsumerGroups()
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

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
