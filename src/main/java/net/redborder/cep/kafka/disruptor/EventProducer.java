package net.redborder.cep.kafka.disruptor;

import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

import java.util.Map;

public class EventProducer {
    private final RingBuffer<MapEvent> ringBuffer;

    public EventProducer(RingBuffer<MapEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorTwoArg<MapEvent, String, Map<String, Object>> TRANSLATOR =
            new EventTranslatorTwoArg<MapEvent, String, Map<String, Object>>()
            {
                @Override
                public void translateTo(MapEvent event, long sequence, String source, Map<String, Object> data)
                {
                    event.setSource(source);
                    event.setData(data);
                }
            };

    public void putData(String source, Map<String, Object> data)
    {
        ringBuffer.publishEvent(TRANSLATOR, source, data);
    }
}
