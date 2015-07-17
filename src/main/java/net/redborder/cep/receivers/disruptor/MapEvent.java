package net.redborder.cep.receivers.disruptor;

import java.util.Map;

public class MapEvent {
    private Map<String, Object> event;

    private String source;

    public void setData(Map<String, Object> event){
        this.event = event;
    }

    public void setSource(String source){
        this.source = source;
    }

    public Map<String, Object> getData(){
        return event;
    }

    public String getSource(){
        return source;
    }
}
