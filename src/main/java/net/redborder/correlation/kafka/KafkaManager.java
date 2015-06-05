package net.redborder.correlation.kafka;

import java.util.*;

public class KafkaManager {
    private static ConsumerManager consumerManager;

    public static void init() {
        consumerManager = new ConsumerManager();
        consumerManager.start(KafkaManager.getTopics());
    }

    private static List<Topic> getTopics(){
        return new ArrayList<>();
    }

    private static void shutdown(){
        consumerManager.shutdown();
    }
}
