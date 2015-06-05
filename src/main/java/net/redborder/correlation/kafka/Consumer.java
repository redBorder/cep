package net.redborder.correlation.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.correlation.kafka.disruptor.EventProducer;
import net.redborder.correlation.kafka.parsers.Parser;

import java.util.Map;

public class Consumer implements Runnable {
    private KafkaStream stream;
    private Topic topic;
    private Parser parser;
    private EventProducer eventProducer;

    public Consumer(KafkaStream stream, Topic topic) {
        this.stream = stream;
        this.topic = topic;
        this.parser = topic.parser;
        this.eventProducer = topic.eventProducer;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            Map<String, Object> event = parser.parse(new String(it.next().message()));

            if (event != null) {
                eventProducer.putData(topic.name, event);
            }
        }
    }
}
