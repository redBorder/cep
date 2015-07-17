package net.redborder.cep.senders;

import java.util.Map;

public interface EventSender {
    void process(String executionPlanId, String streamName, String topic, Map<String, Object> message);
}
