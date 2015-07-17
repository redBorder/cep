package net.redborder.cep.sources;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import net.redborder.cep.sources.disruptor.EventProducer;
import net.redborder.cep.sources.disruptor.MapEvent;
import net.redborder.cep.sources.disruptor.MapEventFactory;
import net.redborder.cep.sources.parsers.ParsersManager;
import net.redborder.cep.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;

public abstract class Source {
    private static final Logger log = LoggerFactory.getLogger(Source.class);
    public EventProducer eventProducer;
    public ParsersManager parsersManager;
    public Map<String, Object> properties;

    public Source(ParsersManager parsersManager, EventHandler eventHandler, Map<String, Object> properties) {
        this.parsersManager = parsersManager;
        this.properties = properties;

        // Create the disruptor for this topic and start it
        Disruptor<MapEvent> disruptor = new Disruptor<>(new MapEventFactory(), ConfigData.getRingBufferSize(), Executors.newCachedThreadPool());
        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        eventProducer = new EventProducer(disruptor.getRingBuffer());
        prepare();
    }

    public abstract void addStreams(String ... streamName);

    public abstract void prepare();

    public abstract void start();

    public abstract void shutdown();

    public void send(String streamName, String msg) {
        Map<String, Object> data = parsersManager.parse(streamName, msg);
        eventProducer.putData(streamName, data);
    }

    public void send(String streamName, Map<String, Object> data) {
        eventProducer.putData(streamName, data);
    }

    public Object getProperty(String propertyName){
        return properties.get(propertyName);
    }
}
