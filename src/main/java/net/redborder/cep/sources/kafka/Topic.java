package net.redborder.cep.sources.kafka;

import net.redborder.cep.sources.Source;
import net.redborder.cep.sources.parsers.Parser;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a kafka topic that will be consumed by KafkaSource.
 * Every topic has a Parser, that will be used to parse messages coming from
 * the topic, and a Source, that will receive every message consumed by the topic.
 *
 * @see Parser
 * @see Source
 */

public class Topic {
    // The name of the topic
    private final String name;

    // The parser that will be used to parse messages coming from this topic
    private final Parser parser;

    // The source that will receive the messages consumed from this topic
    private final Source source;

    // The number of topic's partitions
    private Integer partitions;

    /**
     * Constructs a new topic.
     *
     * @param name The name of the kafka topic
     * @param partitions The number of partitions
     * @param parser The parser that will be used to parse messages coming from this topic
     * @param source The source that will receive the messages consumed from this topic
     */

    public Topic(String name, Integer partitions, Parser parser, Source source) {
        this.name = name;
        this.partitions = partitions;
        this.parser = parser;
        this.source = source;
    }

    /**
     * Returns the number of partitions
     *
     * @return Number of topic partitions
     */

    public Integer getPartitions() {
        return partitions;
    }

    /**
     * Sets the number of partitions
     *
     * @param partitions Number of partitions
     */

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    /**
     * Returns the topic name
     *
     * @return Topic name
     */

    public String getName() {
        return name;
    }

    /**
     * Returns the parser associated with this topic
     *
     * @return Parser associated with this topic
     */

    public Parser getParser() {
        return parser;
    }

    /**
     * Returns the source associated with this topic
     *
     * @return Source associated with this topic
     */

    public Source getSource() {
        return source;
    }

    /**
     * Returns a map representation of the topic, that includes the
     * topic name and the number of partitions.
     *
     * @return Map representation of the topic
     */

    public Map<String, Integer> toMap(){
        Map<String, Integer> hash = new HashMap<>();
        hash.put(name, partitions);
        return hash;
    }

    public String toString() {
        return this.name + "(" + this.partitions + ")";
    }
}
