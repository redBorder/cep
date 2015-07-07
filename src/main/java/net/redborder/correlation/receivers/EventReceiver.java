package net.redborder.correlation.receivers;

import java.util.Map;

public interface EventReceiver {
    void process(String executionPlanId, String streamName, String topic, Map<String, Object> message);
}
