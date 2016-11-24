package net.redborder.cep.sinks;

import java.util.Map;

/**
 * This abstract class represents a output system that will receive every message
 * produced by the CEP, and will output those messages to an external system.
 */

public abstract class Sink {

    // The list of properties associated with this instance on the config file
    public Map<String, Object> properties;

    public Sink(Map<String, Object> properties) {
        this.properties = properties;
    }

    /**
     * This method process the given message from the given stream input, and
     * sends it to a system that will be responsible for storing, persisting and/or
     * processing the event.
     *
     * @param streamName The message input stream
     * @param topic      The destination topic
     * @param message    The message
     */

    public abstract void process(String streamName, String topic, Map<String, Object> message);

    /**
     * This method process the given message from the given stream input, and
     * sends it to a system that will be responsible for storing, persisting and/or
     * processing the event.
     *
     * @param streamName The message input stream
     * @param topic      The destination topic
     * @param key        The message key
     * @param message    The message
     */

    public abstract void process(String streamName, String topic, String key, Map<String, Object> message);

    /**
     * This method starts the sink, requesting resources that may be
     * needed.
     */

    public abstract void start();

    /**
     * This method stops the sink, releasing resources that may have been
     * reserved upon initialization.
     */

    public abstract void shutdown();
}
