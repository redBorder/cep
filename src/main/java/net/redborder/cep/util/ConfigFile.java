package net.redborder.cep.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ho.yaml.Yaml;
import org.ho.yaml.exception.YamlException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class represents a YAML file on the filesystem from
 * which the config values will be read and accessed.
 */

public class ConfigFile {
    private static final Logger log = LogManager.getLogger(ConfigFile.class);

    // The name of the file that will be read
    private final String configFile;

    // The data loaded from the file
    private Map<String, Object> map;

    /**
     * Constructor.
     * Takes the file name to read, and loads the data from it calling the reload method
     *
     * @param configFile The path to the file that will be open
     */

    public ConfigFile(String configFile) {
        this.configFile = configFile;
        reload();
    }

    /**
     * This method updates de instance attribute map with the data loaded
     * from the file configFile
     */

    public void reload() {
        try {
            map = (Map<String, Object>) Yaml.load(new File(configFile));
        } catch (FileNotFoundException e) {
            log.error("Couldn't find config file {}", configFile);
            System.exit(1);
        } catch (YamlException e) {
            log.error("Couldn't read config file {}. Is it a YAML file?", configFile);
            System.exit(1);
        }
    }

    /**
     * Getter.
     * Returns the value associated with the key property on the config file
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
     * Getter with a default value.
     * Returns the value associated with the key property on the config file.
     * If the key does not exist, returns the default value.
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

    /**
     * Gets a list of keys.
     * Given the name of a map property on the config file, returns
     * the list of keys present on that map.
     *
     * @param property Property to read from the general section
     * @return Property read
     */

    public Set<String> getKeys(String property) {
        Map<String, Object> fromFile = get(property);
        Set<String> emptySet = Collections.emptySet();
        return (fromFile == null ? emptySet : fromFile.keySet());
    }
}
