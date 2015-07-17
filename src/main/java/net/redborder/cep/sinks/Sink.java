package net.redborder.cep.sinks;

import java.util.Map;

public interface Sink {
    void process(String streamName, String topic, Map<String, Object> message);
}
