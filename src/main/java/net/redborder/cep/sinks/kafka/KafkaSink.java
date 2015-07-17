package net.redborder.cep.sinks.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.redborder.cep.sinks.Sink;
import net.redborder.cep.util.ConfigData;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    private Producer<String, String> producer;
    private ObjectMapper objectMapper;

    public KafkaSink() {
        objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("metadata.broker.list", ConfigData.getKafkaBrokers());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "60");
        props.put("retry.backoff.ms", "1000");
        props.put("producer.type", "async");
        props.put("queue.buffering.max.messages", "10000");
        props.put("queue.buffering.max.ms", "500");
        props.put("partitioner.class", "net.redborder.cep.sinks.kafka.SimplePartitioner");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<>(config);
    }

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        String clientMac = (String) message.get("client_mac");
        send(topic, clientMac, message);
    }

    @Override
    public void shutdown() {
        producer.close();
    }

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
