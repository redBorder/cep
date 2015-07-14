package net.redborder.cep.siddhi;

import net.redborder.cep.receivers.EventReceiver;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiddhiCallback {
    final EventReceiver eventReceiver;

    public SiddhiCallback(EventReceiver eventReceiver) {
        this.eventReceiver = eventReceiver;
    }

    public StreamCallback getCallback(final String executionPlanId, final String streamName, final String topic, final List<Attribute> attributes) {
        return new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    Map<String, Object> result = new HashMap<>();

                    int index = 0;
                    for (Object object : event.getData()) {
                        String columnName = attributes.get(index++).getName();
                        result.put(columnName, object);
                    }

                    eventReceiver.process(executionPlanId, streamName, topic, result);
                }
            }
        };
    }
}
