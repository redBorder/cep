package net.redborder.cep.sinks.syslog;

import net.redborder.cep.sinks.Sink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static net.redborder.cep.util.Constants.*;

import java.util.Map;

public class SyslogSink extends Sink {

    private static final Logger logger = LogManager.getLogger(SyslogSink.class.getName());

    public SyslogSink(Map<String, Object> properties) {
        super(properties);
    }

    @Override
    public void process(String streamName, String topic, Map<String, Object> message) {
        String key;
        if (message.containsKey(__KEY)) {
            key = (String) message.remove(__KEY);
            process(streamName, topic, key, message);
        } else {
            logger.info("RULE: [{}] ALERT: [{}]", streamName, message);
        }
    }

    @Override
    public void process(String streamName, String topic, String key, Map<String, Object> message) {
        logger.info("RULE: [{}] KEY: [{}] ALERT: [{}]", streamName, key, message);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
