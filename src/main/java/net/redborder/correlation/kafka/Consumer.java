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

    public Consumer(KafkaStream stream, Parser parser) {
        this.stream = stream;
        this.parser = parser;
        this.eventProducer = DisruptorManager.getEventProducer();
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            Map<String, Object> event = parser.parse(new String(it.next().message()));

            if (event != null) {
                eventProducer.putData(event);
            }
        }
    }
}
