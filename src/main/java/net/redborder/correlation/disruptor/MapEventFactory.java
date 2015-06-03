package net.redborder.correlation.disruptor;

import com.lmax.disruptor.EventFactory;

public class MapEventFactory implements EventFactory<MapEvent>{
    @Override
    public MapEvent newInstance() {
        return new MapEvent();
    }
}
