package net.redborder.correlation.util;

import java.util.Collections;
import java.util.Map;

public class ConfigData {
    private static final String CONFIG_FILE_PATH = "/root/correlation_config.yml";
    private static final ConfigFile configFile = new ConfigFile(CONFIG_FILE_PATH);

    private ConfigData() {}

    public static String getZkConnect() {
        return configFile.getOrDefault("zk_connect", "127.0.0.1:2181");
    }

    public static Map<String, String> getTopics() {
        return configFile.getOrDefault("topics", Collections.<String, String>emptyMap());
    }
}
