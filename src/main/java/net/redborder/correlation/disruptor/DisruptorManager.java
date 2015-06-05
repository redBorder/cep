package net.redborder.correlation.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import net.redborder.correlation.siddhi.RbSiddhiManager;

import java.util.concurrent.Executors;

public class DisruptorManager {
    private static Disruptor<MapEvent> disruptor;
    public static EventProducer eventProducer;

    public static void init(Integer ringBufferSize) {
        disruptor = new Disruptor<>(new MapEventFactory(), ringBufferSize, Executors.newCachedThreadPool());
        disruptor.handleEventsWith(RbSiddhiManager.getHandler());
        disruptor.start();
        eventProducer = new EventProducer(getRingBuffer());
    }

    public static EventProducer getEventProducer() {
        return eventProducer;
    }

    public static RingBuffer<MapEvent> getRingBuffer() {
        return disruptor.getRingBuffer();
    }
}
