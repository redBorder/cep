package net.redborder.cep.sinks;

import java.util.Map;

/**
 * This interface represents a output system that will receive every message
 * produced by the CEP, and will output those messages to an external system.
 */

public interface Sink {

    /**
     * This method process the given message from the given stream input, and
     * sends it to a system that will be responsible for storing, persisting and/or
     * processing the event.
     *
     * @param streamName The message input stream
     * @param topic The destination topic
     * @param message The message
     */

    void process(String streamName, String topic, Map<String, Object> message);

    /**
     * This method stops the sink, releasing resources that may have been
     * reserved upon initialization.
     */

    void shutdown();
}
