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

    // Sets the message
    public void setData(Map<String, Object> event){
        this.event = event;
    }

    // Sets the source
    public void setSource(String source){
        this.source = source;
    }

    // Returns the message
    public Map<String, Object> getData(){
        return event;
    }

    // Returns the source
    public String getSource(){
        return source;
    }
}
