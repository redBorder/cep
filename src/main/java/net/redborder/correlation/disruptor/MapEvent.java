package net.redborder.correlation.disruptor;

import java.util.Map;

public class MapEvent {
    private Map<String, Object> event;

    public void set(Map<String, Object> event){
        this.event = event;
    }

    public Map<String, Object>  get(){
        return event;
    }
}
