package net.redborder.correlation.kafka;

import java.util.*;

public class KafkaManager {
    private ConsumerManager consumerManager;
    private List<Topic> topics;

    public KafkaManager() {
        initTopics();
        consumerManager = new ConsumerManager();
        consumerManager.start(getTopics());
    }

    private void initTopics() {
        // TODO initiate kafka topics with partitions.
    }

    public List<Topic> getTopics() {
        return topics;
    }

    private void shutdown(){
        consumerManager.shutdown();
    }
}
