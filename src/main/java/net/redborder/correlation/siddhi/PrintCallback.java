package net.redborder.correlation.siddhi;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrintCallback extends StreamCallback {
    final String id;
    final String streamName;
    final List<Attribute> attributes;

    public PrintCallback(String id, String streamName, List<Attribute> attributes) {
        this.id = id;
        this.streamName = streamName;
        this.attributes = attributes;
    }

    @Override
    public void receive(Event[] events) {
        for (Event event : events) {
            Map<String, Object> result = new HashMap<>();

            int index = 0;
            for (Object object : event.getData()) {
                String columnName = attributes.get(index++).getName();
                result.put(columnName, object);
            }

            System.out.println("[" + id + "] " + result);
        }
    }
}
