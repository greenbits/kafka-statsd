package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/quipo/statsd"
	"github.com/segmentio/go-log"
	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	zkAddrs      = kingpin.Flag("zookeeper-addrs", "Zookeeper addresses (e.g. host1:2181,host2:2181)").Short('z').String()
	statsdAddr   = kingpin.Flag("statsd-addr", "Statsd address").Short('s').String()
	statsdPrefix = kingpin.Flag("statsd-prefix", "Statsd prefix").Short('p').String()
	interval     = kingpin.Flag("refresh-interval", "Interval to refresh offset lag in seconds").Short('i').Default("5").Int()
	tags         = kingpin.Flag("tags", "Use tags if your StatsD client supports them (like DataDog and InfluxDB)").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	statsdClient := statsd.NewStatsdClient(*statsdAddr, *statsdPrefix)
	err := statsdClient.CreateSocket()
	if err != nil {
		log.Error("%s", err)
		return
	}
	stats := statsd.NewStatsdBuffer(time.Second, statsdClient)
	defer stats.Close()

	var zookeeperNodes []string
	zookeeperNodes, chroot := kazoo.ParseConnectionString(*zkAddrs)

	var kz *kazoo.Kazoo
	conf := kazoo.NewConfig()
	conf.Chroot = chroot
	if kz, err = kazoo.NewKazoo(zookeeperNodes, conf); err != nil {
		log.Error("%s", err)
		return
	}
	defer kz.Close()

	brokers, err := kz.BrokerList()
	if err != nil {
		log.Error("%s", err)
		return
	}

	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		log.Error("%s, err")
		return
	}
	defer client.Close()

	consumerGroupList, err := kz.Consumergroups()
	if err != nil {
		log.Error("%s", err)
		return
	}

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-ticker.C:
			log.Info("Refreshing offset lag")

			for _, cg := range consumerGroupList {
				offsets, err := cg.FetchAllOffsets()
				if err != nil {
					log.Error("%s", err)
					return
				}
				for topic, m := range offsets {
					for partitionID, cgOffset := range m {
						tOffset, err := client.GetOffset(topic, partitionID, sarama.OffsetNewest)
						if err != nil {
							log.Error("%s", err)
							return
						}
						lag := tOffset - cgOffset

						log.Info("Topic: %s, Partition: %d, Consumer Group: %s, Lag: %d", topic, partitionID, cg.Name, lag)
						if *tags {
							stats.Gauge(fmt.Sprintf("consumer_lag,topic=%s,partition=%d,consumer_group=%s", topic, partitionID, cg.Name), lag)
						} else {
							stats.Gauge(fmt.Sprintf("topic.%s.partition.%d.consumer_group.%s.lag", topic, partitionID, cg.Name), lag)
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
