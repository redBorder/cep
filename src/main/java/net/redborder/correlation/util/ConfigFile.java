package net.redborder.correlation.util;

import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;

public class ConfigFile {
    private static final Logger log = LoggerFactory.getLogger(ConfigFile.class);
    private final String configFile;
    private Map<String, Object> map;

    public ConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void reload() {
        try {
            map = (Map<String, Object>) Yaml.load(new File(configFile));
        } catch (FileNotFoundException e) {
            log.warn("Couldn't find config file: " + configFile);
            map = Collections.emptyMap();
        }
    }

    /**
     * Getter.
     *
     * @param property Property to read from the general section
     * @return Property read
     */

    public <T> T get(String property) {
        T ret = null;

        if (map != null) {
            ret = (T) map.get(property);
        }

        return ret;
    }

    /**
     * Getter.
     *
     * @param property Property to read from the general section
     *                 If it's not present on the section, give it a default value
     * @return Property read
     */

    public <T> T getOrDefault(String property, T defaultValue) {
        T fromFile = get(property);
        T ret = (fromFile == null ? defaultValue : fromFile);
        return ret;
    }
}
