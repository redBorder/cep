package net.redborder.cep.kafka;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.redborder.cep.kafka.disruptor.EventProducer;
import net.redborder.cep.kafka.disruptor.MapEvent;
import net.redborder.cep.kafka.disruptor.MapEventFactory;
import net.redborder.cep.kafka.parsers.Parser;
import net.redborder.cep.util.ConfigData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);

    private final List<Topic> topics;
    private final List<Topic> unmodifiableTopics;
    private ConsumerConnector consumer;
    private Map<String, ExecutorService> executors = new LinkedHashMap<>();
    private CuratorFramework curator = CuratorFrameworkFactory.newClient(ConfigData.getZkConnect(), new RetryNTimes(10, 30000));

    public ConsumerManager(EventHandler eventHandler) {
        Integer ringBufferSize = ConfigData.getRingBufferSize();
        topics = new ArrayList<>();
        unmodifiableTopics = Collections.unmodifiableList(topics);
        curator.start();

        for (String topicName : ConfigData.getTopics()) {
            String parserName = ConfigData.getParser(topicName);

            try {
                // Get parser from config
                Class parserClass = Class.forName(parserName);
                Constructor<Parser> constructor = parserClass.getConstructor();
                Parser parser = constructor.newInstance();

                // Create the disruptor for this topic and start it
                Disruptor<MapEvent> disruptor = new Disruptor<>(new MapEventFactory(), ringBufferSize, Executors.newCachedThreadPool());
                disruptor.handleEventsWith(eventHandler);
                disruptor.start();

                // Create topic entry
                Integer currentPartitions = 4;

                try {
                    List<String> partitions = curator.getChildren().forPath("/brokers/topics/" + topicName + "/partitions");
                    currentPartitions = partitions.size() != 0 ? partitions.size() : 1;
                } catch (Exception e) {
                    log.warn("Couldn't discover partitions for topic " + topicName, e);
                }

                log.info("Got {} partitions from topic {}", currentPartitions, topicName);
                Topic topic = new Topic(topicName, currentPartitions, parser, new EventProducer(disruptor.getRingBuffer()));
                topics.add(topic);

                initConsumers();

                try {
                    curator.getChildren().usingWatcher(new PartitionsWatcher(topic)).forPath("/brokers/topics/" + topicName + "/partitions");
                } catch (Exception e) {
                    log.warn("Couldn't discover partitions for topic " + topicName, e);
                }
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserName);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserName, e);
            }
        }
    }

    public void initConsumers() {
        Properties props = new Properties();
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", ConfigData.getZkConnect());
        props.put("group.id", "rb-cep-engine");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");

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

    public List<Topic> getTopics() {
        return unmodifiableTopics;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        for (ExecutorService executor : executors.values())
            if (executor != null) executor.shutdown();
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
