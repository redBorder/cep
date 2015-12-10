package net.redborder.cep.sources.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.parsers.Parser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * A thread that will consume messages from a given partition from a topic.
 * This thread will forward the read messages to a source, that gets from a
 * given topic.
 *
 * @see Topic
 * @see Source
 */

public class Consumer implements Runnable {
    private final static Logger log = LogManager.getLogger(Consumer.class);

    // The KafkaStream from where this thread will consume messages
    // This object represents a partition on a topic.
    private KafkaStream stream;

    // The partition's topic
    private Topic topic;

    // The parser that will be used to parse the consumed messages
    private Parser parser;

    // The source that will receive the consumed messages
    private Source source;

    /**
     * Creates a consumer thread.
     *
     * @param stream The KafkaStream that will be used to consume messages
     * @param topic The topic associated to the KafkaStream partition.
     */
    public Consumer(KafkaStream stream, Topic topic) {
        this.stream = stream;
        this.topic = topic;
        this.parser = topic.getParser();
        this.source = topic.getSource();
    }

    /**
     * Starts the consumer thread.
     */

    @Override
    public void run() {
        log.debug("Starting consumer for topic {}", topic);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        // For each message present on the partition...
        while (it.hasNext()) {
            Map<String, Object> event = null;

            // Parse it with the parser associated with the topic
            try {
                event = parser.parse(new String(it.next().message(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            // Send it to the source
            if (event != null) {
                source.send(topic.getName(), event);
            }
        }

        log.debug("Finished consumer for topic {}", topic);
    }
}
