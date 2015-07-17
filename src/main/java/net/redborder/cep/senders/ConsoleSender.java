package net.redborder.cep.senders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsoleSender implements EventSender {
    private static final Logger log = LoggerFactory.getLogger(ConsoleSender.class);

    public ConsoleSender() { }

    @Override
    public void process(String executionPlanId, String streamName, String topic, Map<String, Object> message) {
        log.info("[{}] {}", executionPlanId, message);
    }
}
