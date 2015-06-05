package net.redborder.correlation.kafka;


import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.redborder.correlation.disruptor.DisruptorManager;
import net.redborder.correlation.disruptor.EventProducer;

import java.util.Map;


public class Consumer implements Runnable {

    private KafkaStream stream;
    private Parser parser;
    private EventProducer eventProducer;
    private Topic topic;

    public Consumer(KafkaStream stream, Topic topic) {
        this.stream = stream;
        this.parser = topic.getParser();
        this.topic = topic;
        this.eventProducer = DisruptorManager.getEventProducer(topic.getName());
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            Map<String, Object> event = parser.parse(new String(it.next().message()));

            if (event != null) {
                eventProducer.putData(topic.getName(), event);
            }
        }
    }
}
