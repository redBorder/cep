package net.redborder.correlation.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import net.redborder.correlation.kafka.KafkaManager;
import net.redborder.correlation.kafka.Topic;
import net.redborder.correlation.siddhi.RbSiddhiManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class DisruptorManager {
    public Map<String, EventProducer> eventProducer;

    public DisruptorManager(KafkaManager kafkaManager, Integer ringBufferSize) {
        eventProducer = new ConcurrentHashMap<>();

        for (Topic t : kafkaManager.getTopics()) {
            Disruptor<MapEvent> disruptor = new Disruptor<>(new MapEventFactory(), ringBufferSize, Executors.newCachedThreadPool());
            disruptor.handleEventsWith(RbSiddhiManager.getHandler());
            disruptor.start();
            eventProducer.put(t.getName(), new EventProducer(disruptor.getRingBuffer()));
        }
    }

    public EventProducer getEventProducer(String topic) {
        return eventProducer.get(topic);
    }
}
