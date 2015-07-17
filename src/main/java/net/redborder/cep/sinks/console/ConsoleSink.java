package net.redborder.cep.sinks.console;

import net.redborder.cep.sinks.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsoleSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(ConsoleSink.class);

    public ConsoleSink() { }

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        log.info("[{}] {}", streamName, message);
    }
}
