package net.redborder.cep.receivers;

import java.util.Map;

public interface EventReceiver {
    void process(String executionPlanId, String streamName, String topic, Map<String, Object> message);
}
