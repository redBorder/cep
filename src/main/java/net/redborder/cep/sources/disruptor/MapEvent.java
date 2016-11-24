package net.redborder.cep.sources.disruptor;

import java.util.Map;

/**
 * This class represents an event from a given source
 */

public class MapEvent {
    // The event
    private Map<String, Object> event;

    // The input stream name
    private String source;

    // The message key
    private String key;

    // Sets the message
    public void setData(Map<String, Object> event) {
        this.event = event;
    }

    // Sets the source
    public void setSource(String source) {
        this.source = source;
    }

    // Sets the key
    public void setKey(String key) {
        this.key = key;
    }

    // Returns the message
    public Map<String, Object> getData() {
        return event;
    }

    // Returns the source
    public String getSource() {
        return source;
    }

    // Returns the message key
    public String getKey() {
        return key;
    }
}
