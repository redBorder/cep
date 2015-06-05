package net.redborder.correlation.kafka;

import net.redborder.correlation.kafka.parsers.Parser;
import java.util.HashMap;
import java.util.Map;

public class Topic {
    private String name;
    private Integer partitions;
    private Parser parser;

    public Topic(String name, Integer partitions, Parser parser){
        this.name = name;
        this.partitions = partitions;
        this.parser = parser;
    }

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

    public Parser getParser() {
        return parser;
    }

    public void setParser(Parser parser) {
        this.parser = parser;
    }

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }
}
