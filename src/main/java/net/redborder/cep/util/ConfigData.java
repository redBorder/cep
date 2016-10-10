package net.redborder.cep.util;

import java.util.*;

/**
 * This class serves as an interface for config values from the
 * ConfigFile class associated with it. It mainly lets you access the
 * data available in the config file in an usable and easy way.
 *
 * @see ConfigFile
 */

public class ConfigData {
    // The config file from which this class will read the data
    private static ConfigFile configFile;

    /**
     * Returns the current config file set on this class
     *
     * @return the current config file
     */

    public static ConfigFile getConfigFile() {
        return configFile;
    }

    /**
     * Sets the config file to be used
     *
     * @param fileName The path where the config file is
     */

    public static void setConfigFile(String fileName) {
        configFile = new ConfigFile(fileName);
    }

    // Mark the constructor as private to avoid external instantiation
    private ConfigData() {
    }

    /**
     * Gets the kafka brokers string from the config file on the key
     * "kafka_brokers" formatted as string like the following:
     * "server1:9092,server2:9092". If no kafka brokers have been
     * specified on the config file, it will return a string containing
     * the local server with the default kafka broker port.
     *
     * @return Set of running kafka brokers
     */

    public static String getKafkaBrokers() {
        return configFile.getOrDefault("kafka_brokers", "127.0.0.1:9092");
    }

    /**
     * Gets the path of the state file specified on the config with the
     * key "state_file". If no state file has been set on the config,
     * returns null.
     *
     * @return Path to the state file, or null if not specified
     */

    public static String getStateFile() {
        return configFile.getOrDefault("state_file", null);
    }

    /**
     * Gets the URI specified on the config file under the key "rest_uri".
     * If it is not specified on the config file, returns "http://localhost:8888/myapp/"
     *
     * @return The REST URI specified on config file, or "http://localhost:8888/myapp/"
     * if not specified
     */

    public static String getRESTURI() {
        return configFile.getOrDefault("rest_uri", "http://localhost:8888/myapp/");
    }

    /**
     * Gets the size of the buffer ring specified on the config file under
     * the key "ring_buffer_size". If it isn't specified, returns 1024.
     *
     * @return The ring buffer size specified on the config file, or 1024 if not specified
     */

    public static Integer getRingBufferSize() {
        return configFile.getOrDefault("ring_buffer_size", 1024);
    }

    /**
     * Gets the set of input streams specified on the config file
     *
     * @return A set with the input streams specified on the config file.
     */

    public static Set<String> getStreams() {
        return configFile.getKeys("streams");
    }

    /**
     * Gets the sources specified on the config file as a list of maps, where
     * each map has information about the name of the source, and the class that
     * implements it.
     *
     * @return A list of maps, where each map is a source with name and class.
     */

    public static List<Map<String, Object>> getSources() {
        return configFile.getOrDefault("sources", new ArrayList<Map<String, Object>>());
    }

    /**
     * Gets the sinks specified on the config file as a list of maps, where
     * each map has information about the name of the sink, the class that
     * implements it and it's properties.
     *
     * @return A list of maps, where each map is a source with name, class and properties.
     */

    public static List<Map<String, Object>> getSinks() {
        return configFile.getOrDefault("sinks", new ArrayList<Map<String, Object>>());
    }

    /**
     * Gets the map of parsers specified on the config file. The key of each
     * entry is the parser name, and the value associated is the class that
     * will implement that parser.
     *
     * @return A map of parsers data
     */

    public static Map<String, String> getParsers() {
        Map<String, String> parsers = configFile.get("parsers");

        if (parsers == null)
            parsers = new HashMap<>();

        return parsers;
    }

    /**
     * Gets the source name associated with a stream.
     *
     * @param streamName The stream name
     * @return The source name associated with the given stream.
     */

    @SuppressWarnings("unchecked")
    public static String getSource(String streamName) {
        Map<String, Object> topics = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) topics.get(streamName);
        return (String) streamData.get("source");
    }

    /**
     * Gets the parser associated with a stream.
     *
     * @param streamName The stream name
     * @return The parser name associated with the given stream.
     */

    @SuppressWarnings("unchecked")
    public static String getParser(String streamName) {
        Map<String, Object> streams = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) streams.get(streamName);
        return (String) streamData.get("parser");
    }

    /**
     * Gets the attributes map associated with a stream. Each entry of this map
     * is a attribute associated with the stream. The key of the entry is the
     * attribute name, and the value associated with it is the type of the attribute
     *
     * @param streamName The stream name
     * @return The attributes associated with the given stream.
     */

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAttributes(String streamName) {
        Map<String, Object> topics = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) topics.get(streamName);
        Map<String, String> attributes = (Map<String, String>) streamData.get("attributes");

        //Add the __KEY attribute in order to produce keyed messages
        attributes.put("__KEY", "string");
        return attributes;
    }
}
