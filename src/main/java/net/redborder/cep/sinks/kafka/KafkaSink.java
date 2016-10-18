package net.redborder.cep.sinks.kafka;

import net.redborder.cep.sinks.Sink;
import net.redborder.cep.util.ConfigData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static net.redborder.cep.util.Constants.*;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class implements a Sink that sends messages to Apache Kafka.
 * KafkaSink will send each message to the kafka topic associated with
 * each message.
 *
 * @see Sink
 */

public class KafkaSink extends Sink {
    private static final Logger log = LogManager.getLogger(KafkaSink.class);

    //Closed boolean
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // The kafka producer
    private KafkaProducer<String, String> producer;

    // A JSON parser from Jackson
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Creates a new KafkaSink.
     * This method initializes and starts a new Kafka producer that will be
     * used to produce messages to kafka topics.
     *
     * @param properties KafkaSink propertiers. You should provide a kafka_broker property
     *                   set to the Kafka host address. If none is provided localhost will
     *                   be used
     */

    public KafkaSink(Map<String, Object> properties) {
        super(properties);
        // The producer config attributes
        Properties props = new Properties();
        if (properties != null && properties.get("kafka_brokers") != null) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka_brokers"));
        } else {
            props.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        }
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(PARTITIONER_CLASS_CONFIG, "net.redborder.cep.sinks.kafka.SimplePartitioner");

        // Initialize the producer
        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void start() {
    }

    /**
     * This method sends the given message to a given kafka topic.
     *
     * @param streamName The message input stream (unused)
     * @param topic      The destination topic
     * @param message    The message
     */

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        if (!closed.get()) {
            String key = (String) message.get("client_mac");
            if (message.containsKey(__KEY)) {
                key = (String) message.remove(__KEY);
                process(streamName, topic, key, message);
            } else {
                send(topic, key, message);
            }
        }
    }

    /**
     * This method sends the given message to a given kafka topic.
     *
     * @param streamName The message input stream (unused)
     * @param topic      The destination topic
     * @param key        The message key
     * @param message    The message
     */
    @Override
    public void process(String streamName, String topic, String key, Map<String, Object> message) {
        send(topic, key, message);
    }

    /**
     * Stops the kafka producer and releases its resources.
     */

    @Override
    public void shutdown() {
        closed.set(true);
        producer.flush();
        producer.close();
    }

    /**
     * This method sends a given message, with a given key to a given kafka topic.
     *
     * @param topic   The topic where the message will be sent
     * @param key     The key of the message
     * @param message The message to send
     */

    public void send(String topic, String key, Map<String, Object> message) {
        try {
            String messageStr = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messageStr);
            producer.send(record);
        } catch (IOException e) {
            log.error("Error converting map to json: {}", message);
        }
    }
}
