package net.redborder.cep.sources.kafka;


import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.parsers.Parser;
import net.redborder.cep.util.ConfigData;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread that will consume messages from a given partition from a topic.
 * This thread will forward the read messages to a source, that gets from a
 * given topic.
 *
 * @see Topic
 * @see Source
 */

public class Consumer extends Thread {
    private final static Logger log = LogManager.getLogger(Consumer.class);


    // The partition's topic
    private Topic topic;

    // The parser that will be used to parse the consumed messages
    private Parser parser;

    // The source that will receive the consumed messages
    private Source source;

    private KafkaConsumer<String, String> consumer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Integer threadId;


    /**
     * Creates a consumer thread.
     *
     * @param threadId The thread ID
     * @param topic    The topic associated to the KafkaStream partition.
     */
    public Consumer(Integer threadId, Topic topic, String kafka_brokers) {
        this.topic = topic;
        this.parser = topic.getParser();
        this.source = topic.getSource();
        this.threadId = threadId;

        Properties props = new Properties();
        props.put("bootstrap.servers", kafka_brokers);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(GROUP_ID_CONFIG, "rb-cep-engine");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(AUTO_OFFSET_RESET_CONFIG, ConfigData.getConfigFile().getOrDefault("kafka_offset", "latest"));
        consumer = new KafkaConsumer<>(props);
    }

    /**
     * Starts the consumer thread.
     */

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic.getName()), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Removing  : " + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Adding    : " + partitions);
            }
        });

        log.debug("Starting consumer for topic {}, thread: {}", topic, threadId);

        while (!closed.get()) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> event;
                // Parse it with the parser associated with the topic
                event = parser.parse(record.value());
                // Send it to the source
                if (event != null) {
                    source.send(topic.getName(), record.key(), event);
                }
            }
        }

        log.debug("Finished consumer for topic {}", topic);
    }

    public void shutdown() {
        System.out.println("Closing THREAD[" + threadId + "]");
        closed.set(true);
        consumer.wakeup();
    }
}
