package net.redborder.cep.sources.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * A factory class that creates new events for LMAX disruptor
 */

public class MapEventFactory implements EventFactory<MapEvent> {
    @Override
    public MapEvent newInstance() {
        return new MapEvent();
    }
}
