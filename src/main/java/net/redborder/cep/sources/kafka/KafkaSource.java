package net.redborder.cep.sources.kafka;

import com.lmax.disruptor.EventHandler;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.parsers.ParsersManager;
import net.redborder.cep.util.ConfigData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaSource extends Source {
    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
    private ConsumerConnector consumer;
    private Map<String, ExecutorService> executors = new LinkedHashMap<>();
    private List<Topic> topics = new ArrayList<>();
    private Properties props;
    private CuratorFramework curator;

    public KafkaSource(ParsersManager parsersManager, EventHandler eventHandler, Map<String, Object> properties) {
        super(parsersManager, eventHandler, properties);
    }

    @Override
    public void addStreams(String... streamNames) {
        for (String streamName : streamNames) {
            // Create topic entry
            Integer currentPartitions = 4;

            try {
                List<String> partitions = curator.getChildren().forPath("/brokers/topics/" + streamName + "/partitions");
                currentPartitions = partitions.size() != 0 ? partitions.size() : 1;
            } catch (Exception e) {
                log.warn("Couldn't discover partitions for topic " + streamName, e);
            }

            Topic topic = new Topic(streamName, currentPartitions, parsersManager.getParserByStream(streamName), this);
            topics.add(topic);

            try {
                curator.getChildren().usingWatcher(new PartitionsWatcher(topic)).forPath("/brokers/topics/" + streamName + "/partitions");
            } catch (Exception e) {
                log.warn("Couldn't discover partitions for topic " + streamName, e);
            }
        }
    }

    @Override
    public void prepare() {
        curator = CuratorFrameworkFactory.newClient(ConfigData.getZkConnect(), new RetryNTimes(10, 30000));
        curator.start();

        props = new Properties();
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", ConfigData.getZkConnect());
        props.put("group.id", "rb-cep-engine");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");
    }

    @Override
    public void start() {
        initConsumers();
    }

    private void initConsumers() {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        executors.clear();

        Map<String, Integer> topicsHash = new HashMap<>();
        log.info("Starting with topics {}", topics);

        for (Topic topic : topics) {
            topicsHash.putAll(topic.toMap());
        }

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicsHash);

        for (Topic topic : topics) {
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic.getName());
            ExecutorService executor = Executors.newFixedThreadPool(topic.getPartitions());

            for (final KafkaStream stream : streams) {
                executor.submit(new Consumer(stream, topic));
            }

            executors.put(topic.getName(), executor);
        }
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        for (ExecutorService executor : executors.values()) {
            if (executor != null) executor.shutdown();
        }
        curator.close();
    }

    private class PartitionsWatcher implements CuratorWatcher {
        private Topic topic;

        public PartitionsWatcher(Topic topic) {
            this.topic = topic;
        }

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                log.info("Kafka partitions changed! Sleeping 3 secs to catch up with the partitions");
                Thread.sleep(3000);

                List<String> partitions = curator.getChildren().forPath("/brokers/topics/" + topic.getName() + "/partitions");
                Integer currentPartitions = partitions.size();

                if (!currentPartitions.equals(topic.getPartitions())) {
                    log.info("Topic {} partitions changed. Old partitions {}, new partitions {}", topic.getName(), topic.getPartitions(), currentPartitions);

                    log.info("Shutting down consumers...");
                    ExecutorService executorService = executors.get(topic.getName());
                    executorService.shutdown();
                    log.info("Awaiting consumers termination...");
                    executorService.awaitTermination(5, TimeUnit.SECONDS);
                    consumer.shutdown();
                    log.info("Done!");

                    // Set the new partitions and start the consumers again
                    topic.setPartitions(currentPartitions);
                    initConsumers();
                }
            }

            curator.getChildren().usingWatcher(new PartitionsWatcher(topic)).forPath("/brokers/topics/" + topic.getName() + "/partitions");
        }
    }
}
