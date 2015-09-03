package net.redborder.cep.siddhi;

import net.redborder.cep.sinks.Sink;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used as an callback factory to produce messages from
 * Siddhi to Sinks.
 */

public class SiddhiCallback {
    final Sink eventReceiver;

    /**
     * Creates a new factory that will produce callbacks that send events
     * to the given sink.
     *
     * @param eventReceiver The sink that will receive the events
     */

    public SiddhiCallback(Sink eventReceiver) {
        this.eventReceiver = eventReceiver;
    }

    /**
     * Gets the callback that Siddhi will use to output messages
     *
     * @param streamName The stream that produced the message
     * @param topic The destination topic for that message
     * @param attributes Attributes from the message, which stores both attributes names and values.
     * @return A StreamCallback that Siddhi will use to send events
     */

    public StreamCallback getCallback(final String streamName, final String topic, final List<Attribute> attributes) {
        return new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    // This map will store the final message
                    Map<String, Object> result = new HashMap<>();

                    // Get all the attributes values from the list of attributes
                    int index = 0;
                    for (Object object : event.getData()) {
                        String columnName = attributes.get(index++).getName();
                        result.put(columnName, object);
                    }

                    // Send the message to the sink
                    eventReceiver.process(streamName, topic, result);
                }
            }
        };
    }
}
