package net.redborder.cep.util;

import java.util.Map;
import java.util.Set;

public class ConfigData {
    private static ConfigFile configFile;

    private ConfigData() {}

    public static ConfigFile getConfigFile() {
        return configFile;
    }

    public static void setConfigFile(String fileName) {
        configFile = new ConfigFile(fileName);
    }

    public static String getZkConnect() {
        return configFile.getOrDefault("zk_connect", "127.0.0.1:2181");
    }

    public static String getKafkaBrokers() {
        return configFile.getOrDefault("kafka_brokers", "127.0.0.1:9092");
    }

    public static String getStateFile() {
        return configFile.getOrDefault("state_file", null);
    }

    public static String getRESTURI() {
        return configFile.getOrDefault("rest_uri", "http://localhost:8888/myapp/");
    }

    public static Integer getRingBufferSize() {
        return configFile.getOrDefault("ring_buffer_size", 1024);
    }

    public static Set<String> getStreams() {
        return configFile.getKeys("streams");
    }

    public static Map<String, String> getSources(){
        return configFile.get("sources");
    }

    public static Map<String, String> getParsers(){
        return configFile.get("parsers");
    }

    @SuppressWarnings("unchecked")
    public static String getParser(String streamName) {
        Map<String, Object> streams = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) streams.get(streamName);
        return (String) streamData.get("parser");
    }

    @SuppressWarnings("unchecked")
    public static String getSource(String streamName) {
        Map<String, Object> topics = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) topics.get(streamName);
        return (String) streamData.get("source");
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAttributes(String topicName) {
        Map<String, Object> topics = configFile.get("streams");
        Map<String, Object> streamData = (Map<String, Object>) topics.get(topicName);
        return (Map<String, String>) streamData.get("attributes");
    }
}
