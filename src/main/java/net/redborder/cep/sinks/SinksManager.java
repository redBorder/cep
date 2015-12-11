package net.redborder.cep.sinks;


import net.redborder.cep.sinks.console.ConsoleSink;
import net.redborder.cep.util.ConfigData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class serves as a manager for the different implementations
 * of sinks. It is in charge of creating an instance of each of the
 * sinks available on the config file and to add the streams associated
 * with each of the sinks.
 *
 * @see Sink
 */

public class SinksManager {
    private static final Logger log = LogManager.getLogger(SinksManager.class);

    /**
     * This attribute stores a reference to each of the sinks present
     * on the config file as an entry on a map. The entry key is the sink
     * name as specified on the config file, and the value associated with it
     * is an instance of that Sink, that is created with the Java Reflection API,
     * based on the fully qualified class name specified on the config file for
     * that specific sink.
     */

    private Map<String, Sink> sinks = new HashMap<>();

    /**
     * This method creates the sink instances as specified on the config file, and stores
     * them on the 'sinks' attribute. It also starts each one of the sink instances.
     */

    public SinksManager() {
        // For each sources specified on the config file...
        for (Map<String, Object> sinkEntry : ConfigData.getSinks()) {
            // Get its name, class and properties
            String sinkName = (String) sinkEntry.get("name");
            String sinkNameClass = (String) sinkEntry.get("class");
            Map<String, Object> properties = (Map<String, Object>) sinkEntry.get("properties");

            try {
                // Get the class from the full qualified class name and instanciate it
                Class sinkClass = Class.forName(sinkNameClass);
                Constructor<Sink> constructor = sinkClass.getConstructor(Map.class);
                Sink sink = constructor.newInstance(new Object[]{properties});
                sinks.put(sinkName, sink);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the source " + sinkNameClass);
            } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the source " + sinkNameClass, e);
            }
        }

        // Start all the sinks
        start();
    }

    private SinksManager(String sinkName, Sink sink){
        sinks.put(sinkName, sink);
        start();
    }

    /**
    * Create a new SinksManager with a Console Sink as the sole Sink
    * */

    public static SinksManager withConsoleSink(){
        return new SinksManager("console", new ConsoleSink(null));
    }

    /**
     * Sends a start signal to all the sinks instantiated, so they
     * can start.
     */

    public void start() {
        for (Sink s : sinks.values()) {
            if (s != null) {
                s.start();
            }
        }
    }

    /**
     * Sends a shutdown signal to all the sinks instantiated, so they
     * can close connections or release resources.
     */

    public void shutdown() {
        for (Sink sink : sinks.values()) {
            sink.shutdown();
        }
    }

    /**
     * Sends a process signal to all the sinks instantiated.
     */

    public void process(String streamName, String topic, Map<String, Object> message) {
        for (Sink sink : sinks.values()) {
            sink.process(streamName, topic, message);
        }
    }

}
