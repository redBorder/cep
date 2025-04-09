package net.redborder.cep.sources.kafka;

import com.lmax.disruptor.EventHandler;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.parsers.ParsersManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class implements a Source that receives events from a Kafka
 * cluster. It reads the list of brokers available from zookeeper using
 * the config file property zk_connect.
 */

public class KafkaSource extends Source {
    private static final Logger log = LogManager.getLogger(KafkaSource.class);

    // The Kafka Consumer Object API
    private ConsumerConnector consumer;

    // A map that stores one executor services for each topic that
    // will be read from kafka. Each executor service will execute
    // a set of threads, one for each partitions on each topic.
    private Map<String, ExecutorService> executors = new LinkedHashMap<>();

    // The list of topics that must be read from Kafka
    private List<Topic> topics = new ArrayList<>();

    // The consumer properties
    private Properties props;

    // The Apache Curator instance, in order to connect with ZooKeeper
    private CuratorFramework curator;

    // Do nothing, just call the parent constructor
    public KafkaSource(ParsersManager parsersManager, EventHandler eventHandler, Map<String, Object> properties) {
        super(parsersManager, eventHandler, properties);
    }

    /**
     * This method prepares the properties that will be used to
     * consume messages from Kafka, and will start the Zookeeper connection.
     */

    @Override
    public void prepare() {
        String zkConnect = (String) getProperty("zk_connect");

        curator = CuratorFrameworkFactory.newClient(zkConnect, new RetryNTimes(10, 30000));
        curator.start();

        props = new Properties();
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", "rb-cep-engine");
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");
    }


    /**
     * This method gets the list of partitions available for the set of topics given
     * from Zookeeper, creates a new Topic instance for each of them, and stores those
     * instances for later use.
     *
     * @param topicNames An array of Kafka topic names
     */

    @Override
    public void addStreams(String... topicNames) {
        // For each topic name...
        for (String topicName : topicNames) {
            // Fallback partitions number in case we can't get them from ZooKeeper.
            Integer currentPartitions = 4;

            try {
                // Get the number of partitions registered on zookeeper for that topic
                List<String> partitions = curator.getChildren().forPath("/brokers/topics/" + topicName + "/partitions");
                currentPartitions = partitions.size() != 0 ? partitions.size() : 1;
            } catch (Exception e) {
                log.warn("Couldn't discover partitions for topic " + topicName, e);
            }

            // Create the topic with the name and the number of partitions and store the reference
            Topic topic = new Topic(topicName, currentPartitions, parsersManager.getParserByStream(topicName), this);
            topics.add(topic);

            try {
                // Set a watcher on the partitions path, to update the topic partitions
                // in case some partitions are created.
                curator.getChildren().usingWatcher(new PartitionsWatcher(topic)).forPath("/brokers/topics/" + topicName + "/partitions");
            } catch (Exception e) {
                log.warn("Couldn't discover partitions for topic " + topicName, e);
            }
        }
    }

    /**
     * This method starts the consumer threads for all the topics that
     * have been specified with the addStreams method. After it has been called,
     * messages from those topics are being effectively read from Kafka.
     */

    @Override
    public void start() {
        initConsumers();
    }

    /**
     * This method initializes the consumer threads for all topics.
     * It creates one executor service for each topic, and it creates and runs one
     * thread for each partition on that topic.
     */

    private void initConsumers() {
        // Create the Kafka consumers connectors and remove any executors that may be present
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        executors.clear();

        Map<String, Integer> topicsHash = new HashMap<>();
        log.info("Starting with topics {}", topics);

        for (Topic topic : topics) {
            topicsHash.putAll(topic.toMap());
        }

        // Gets the list of KafkaStreams (partitions) associated with each topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicsHash);

        // For each topic...
        for (Topic topic : topics) {
            // Get the list of partitions
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic.getName());

            // Create a new Executor Service
            ExecutorService executor = Executors.newFixedThreadPool(topic.getPartitions());

            // Send and start a thread for each partition and schedule it on the executor service
            for (final KafkaStream stream : streams) {
                executor.submit(new Consumer(stream, topic));
            }

            // Save the Executor Service for later use
            executors.put(topic.getName(), executor);
        }
    }

    /**
     * Shutdowns the executor service for each topic, that will stop and delete
     * all of the consumer threads associated with them, effectively stopping all
     * the consumers. After calling this method, no message will be sent by this source.
     */

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        for (ExecutorService executor : executors.values()) {
            if (executor != null) executor.shutdown();
        }
        curator.close();
    }

    /**
     * This class guarantees that KafkaSource reads from all the partitions in case
     * the kafka partitions for that topic changes. It watches the zookeeper path that stores
     * the partitions for a given topic. When the partitions list changes, Zookeeper calls
     * the process function, that will create the consumer threads again.
     */

    private class PartitionsWatcher implements CuratorWatcher {
        // The partitions' topics that this watcher will watch
        private Topic topic;

        public PartitionsWatcher(Topic topic) {
            this.topic = topic;
        }

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            // If the children of the path changed...
            if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                // First, lets sleep for three seconds to guarantee that the process of partition
                // balancing has finished completely.
                log.info("Kafka partitions changed! Sleeping 3 secs to catch up with the partitions");
                Thread.sleep(3000);

                // Get the partitions from the path
                List<String> partitions = curator.getChildren().forPath("/brokers/topics/" + topic.getName() + "/partitions");
                Integer currentPartitions = partitions.size();

                // If the partitions number have changed, stop and delete all the threads
                // that were consuming that topic, and start all of them again.
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

            // Since the watchers are removed when they are fired, we set a watcher on the
            // same path again.
            curator.getChildren().usingWatcher(new PartitionsWatcher(topic)).forPath("/brokers/topics/" + topic.getName() + "/partitions");
        }
    }
}
