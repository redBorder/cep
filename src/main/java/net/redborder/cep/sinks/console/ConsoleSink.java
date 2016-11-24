package net.redborder.cep.sinks.console;

import net.redborder.cep.sinks.Sink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static net.redborder.cep.util.Constants.*;

import java.util.Map;

/**
 * This class implements a Sink that outputs the messages to the
 * logging system as an info message.
 */

public class ConsoleSink extends Sink {
    private static final Logger log = LogManager.getLogger(ConsoleSink.class);

    /**
     * Creates a new ConsoleSink. A ConsoleSink outputs the messages to STDOUT via
     * the logger with INFO as the log level.
     *
     * @param properties There are no properties to set for a ConsoleSink, can be null.
     */

    public ConsoleSink(Map<String, Object> properties) {
        super(properties);
    }

    /**
     * Logs the message with the logging system as an info message.
     *
     * @param streamName The message input stream (unused)
     * @param topic      The destination topic (unused)
     * @param message    The message
     */

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        String key;
        if (message.containsKey(__KEY)) {
            key = (String) message.remove(__KEY);
            process(streamName, topic, key, message);
        } else {
            log.info("[{}] {}", streamName, message);
        }
    }

    @Override
    public void process(String streamName, String topic, String key, Map<String, Object> message) {
        log.info("[{}] {} {}", streamName, key, message);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
    }
}
