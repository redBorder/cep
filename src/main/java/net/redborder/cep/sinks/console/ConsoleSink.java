package net.redborder.cep.sinks.console;

import net.redborder.cep.sinks.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class implements a Sink that outputs the messages to the
 * logging system as an info message.
 */

public class ConsoleSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(ConsoleSink.class);

    public ConsoleSink() { }

    /**
     * Logs the message with the logging system as an info message.
     *
     * @param streamName The message input stream (unused)
     * @param topic The destination topic (unused)
     * @param message The message
     */

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        log.info("[{}] {}", streamName, message);
    }

    @Override
    public void shutdown() { }
}
