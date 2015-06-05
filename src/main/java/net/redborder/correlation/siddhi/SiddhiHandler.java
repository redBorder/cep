package net.redborder.correlation.siddhi;

import com.lmax.disruptor.EventHandler;
import net.redborder.correlation.disruptor.MapEvent;

public class SiddhiHandler implements EventHandler<MapEvent> {
    @Override
    public void onEvent(MapEvent mapEvent, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("Handler: " + mapEvent.getData().toString());
    }
}
