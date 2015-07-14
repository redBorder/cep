package net.redborder.cep.receivers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsoleReceiver implements EventReceiver {
    private static final Logger log = LoggerFactory.getLogger(ConsoleReceiver.class);

    public ConsoleReceiver() { }

    @Override
    public void process(String executionPlanId, String streamName, String topic, Map<String, Object> message) {
        log.info("[{}] {}", executionPlanId, message);
    }
}
