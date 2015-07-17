package net.redborder.cep.sources;

import com.lmax.disruptor.EventHandler;
import net.redborder.cep.sources.parsers.ParsersManager;
import net.redborder.cep.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class SourcesManager {
    private static final Logger log = LoggerFactory.getLogger(SourcesManager.class);
    private Map<String, Source> sources = new HashMap<>();

    public SourcesManager(ParsersManager parsersManager, EventHandler eventHandler) {

        for (Map.Entry<String, String> sourceEntry : ConfigData.getSources().entrySet()) {
            try {
                // Get parser from config
                Class sourceClass = Class.forName(sourceEntry.getValue());
                Constructor<Source> constructor = sourceClass.getConstructor(ParsersManager.class, EventHandler.class);
                Source source = constructor.newInstance(new Object[]{parsersManager, eventHandler});
                sources.put(sourceEntry.getKey(), source);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the source " + sourceEntry.getValue());
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the source " + sourceEntry.getValue(), e);
            }
        }

        for (String streamName : ConfigData.getStreams()) {
            String sourceName = ConfigData.getSource(streamName);
            Source source = sources.get(sourceName);
            if (source != null) {
                source.addStreams(streamName);
            }
        }

        for (Source source : sources.values()) {
            if (source != null) {
                source.start();
            }
        }
    }

    public void shutdown() {
        for (Source receiver : sources.values()) {
            receiver.shutdown();
        }
    }
}


