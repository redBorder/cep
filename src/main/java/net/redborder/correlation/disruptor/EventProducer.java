package net.redborder.correlation.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;

import java.util.Map;

public class EventProducer {
    private final RingBuffer<MapEvent> ringBuffer;

    public EventProducer(RingBuffer<MapEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<MapEvent, Map<String, Object>> TRANSLATOR =
            new EventTranslatorOneArg<MapEvent, Map<String, Object>>()
            {
                public void translateTo(MapEvent event, long sequence, Map<String, Object> map)
                {
                    event.set(map);
                }
            };

    public void putData(Map<String, Object> map)
    {
        ringBuffer.publishEvent(TRANSLATOR, map);
    }
}
