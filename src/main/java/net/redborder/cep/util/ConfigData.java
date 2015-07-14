package net.redborder.cep.util;

import java.util.Map;
import java.util.Set;

public class ConfigData {
    private static String CONFIG_FILE_PATH = "config.yml";
    private static ConfigFile configFile = new ConfigFile(CONFIG_FILE_PATH);

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

    public static Set<String> getTopics() {
        return configFile.getKeys("topics");
    }

    @SuppressWarnings("unchecked")
    public static String getParser(String topicName) {
        Map<String, Object> topics = configFile.get("topics");
        Map<String, Object> topicData = (Map<String, Object>) topics.get(topicName);
        return (String) topicData.get("parser");
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getAttributes(String topicName) {
        Map<String, Object> topics = configFile.get("topics");
        Map<String, Object> topicData = (Map<String, Object>) topics.get(topicName);
        return (Map<String, String>) topicData.get("attributes");
    }
}
