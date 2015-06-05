package net.redborder.correlation.kafka;

import net.redborder.correlation.kafka.parsers.Parser;
import java.util.HashMap;
import java.util.Map;

public class Topic<T> {
    private String name;
    private Integer partitions;
    private Parser<T> parser;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Parser<T> getParser() {
        return parser;
    }

    public void setParser(Parser<T> parser) {
        this.parser = parser;
    }

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }
}
