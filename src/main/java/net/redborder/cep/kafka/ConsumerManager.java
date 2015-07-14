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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(ConsumerManager.class);

    private final List<Topic> topics;
    private final List<Topic> unmodifiableTopics;
    private ConsumerConnector consumer;
    private Map<String, ExecutorService> executors;

    public ConsumerManager(EventHandler eventHandler) {
        Integer ringBufferSize = ConfigData.getRingBufferSize();
        topics = new ArrayList<>();
        unmodifiableTopics = Collections.unmodifiableList(topics);

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
                /* TODO Discover partitions using ZK */
                topics.add(new Topic(topicName, 4, parser, new EventProducer(disruptor.getRingBuffer())));
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the parser " + parserName);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the parser " + parserName, e);
            }
        }

        Properties props = new Properties();
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.connect", ConfigData.getZkConnect());
        props.put("group.id", "rb-cep-engine");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "60000");
        props.put("auto.offset.reset", "largest");

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        executors = new LinkedHashMap<>();

        Map<String, Integer> topicsHash = new HashMap<>();
        log.info("Starting with topics {}", topics);

        for(Topic topic : topics) {
            topicsHash.putAll(topic.toMap());
        }

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicsHash);

        for(Topic topic : topics) {
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
    }
}
