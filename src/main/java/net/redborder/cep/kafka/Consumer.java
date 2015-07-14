package net.redborder.cep.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.cep.kafka.disruptor.EventProducer;
import net.redborder.cep.kafka.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class Consumer implements Runnable {
    private final static Logger log = LoggerFactory.getLogger(Consumer.class);

    private KafkaStream stream;
    private Topic topic;
    private Parser parser;
    private EventProducer eventProducer;

    public Consumer(KafkaStream stream, Topic topic) {
        this.stream = stream;
        this.topic = topic;
        this.parser = topic.getParser();
        this.eventProducer = topic.getEventProducer();
    }

    @Override
    public void run() {
        log.debug("Starting consumer for topic {}", topic);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            Map<String, Object> event = null;

            try {
                event = parser.parse(new String(it.next().message(), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            if (event != null) {
                eventProducer.putData(topic.getName(), event);
            }
        }

        log.debug("Finished consumer for topic {}", topic);
    }
}
