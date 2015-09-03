package net.redborder.cep.sources.disruptor;

import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

import java.util.Map;

/**
 * This class represents a producer than can write in a thread-safe
 * environment into the ring buffer.
 */

public class EventProducer {
    // The ring buffer where the messages will be written
    private final RingBuffer<MapEvent> ringBuffer;

    /**
     * Creates a new producer for the given ring buffer.
     *
     * @param ringBuffer The ring buffer where the producer will write
     */

    public EventProducer(RingBuffer<MapEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    /**
     * For each event published into the ring buffer, LMAX Disruptor will create a Map Event
     * with the factory MapEventFactory and call this method to transform the published message
     * into a MapEvent message.
     */

    private static final EventTranslatorTwoArg<MapEvent, String, Map<String, Object>> TRANSLATOR =
        new EventTranslatorTwoArg<MapEvent, String, Map<String, Object>>() {
            @Override
            public void translateTo(MapEvent event, long sequence, String source, Map<String, Object> data) {
                event.setSource(source);
                event.setData(data);
            }
        };

    /**
     * Publish a new message into the ring buffer
     * @param source The input stream name
     * @param data The message
     */

    public void putData(String source, Map<String, Object> data) {
        ringBuffer.publishEvent(TRANSLATOR, source, data);
    }
}
