package net.redborder.cep.sinks.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.redborder.cep.sinks.Sink;
import net.redborder.cep.util.ConfigData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * This class implements a Sink that sends messages to Apache Kafka.
 * KafkaSink will send each message to the kafka topic associated with
 * each message.
 *
 * @see Sink
 */

public class KafkaSink extends Sink {
    private static final Logger log = LogManager.getLogger(KafkaSink.class);

    // The kafka producer
    private Producer<String, String> producer;

    // A JSON parser from Jackson
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Creates a new KafkaSink.
     * This method initializes and starts a new Kafka producer that will be
     * used to produce messages to kafka topics.
     * @param properties KafkaSink propertiers. You should provide a kafka_broker property
     *                   set to the Kafka host address. If none is provided localhost will
     *                   be used
     */

    public KafkaSink(Map<String, Object> properties) {
        super(properties);
        // The producer config attributes
        Properties props = new Properties();
        if (properties != null && properties.get("kafka_brokers") != null)
            props.put("metadata.broker.list", properties.get("kafka_brokers"));
        else
            props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "60");
        props.put("retry.backoff.ms", "1000");
        props.put("producer.type", "async");
        props.put("queue.buffering.max.messages", "10000");
        props.put("queue.buffering.max.ms", "500");
        props.put("partitioner.class", "net.redborder.cep.sinks.kafka.SimplePartitioner");

        // Initialize the producer
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
    }

    @Override
    public void start() {
    }

    /**
     * This method sends the given message to a given kafka topic.
     *
     * @param streamName The message input stream (unused)
     * @param topic The destination topic
     * @param message The message
     */

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        String clientMac = (String) message.get("client_mac");
        send(topic, clientMac, message);
    }

    /**
     * Stops the kafka producer and releases its resources.
     */

    @Override
    public void shutdown() {
        producer.close();
    }

    /**
     * This method sends a given message, with a given key to a given kafka topic.
     *
     * @param topic The topic where the message will be sent
     * @param key The key of the message
     * @param message The message to send
     */

    public void send(String topic, String key, Map<String, Object> message) {
        try {
            String messageStr = objectMapper.writeValueAsString(message);
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, key, messageStr);
            producer.send(keyedMessage);
        } catch (IOException e) {
            log.error("Error converting map to json: {}", message);
        }
    }
}
