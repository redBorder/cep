package net.redborder.correlation;

import net.redborder.correlation.disruptor.DisruptorManager;
import net.redborder.correlation.kafka.KafkaManager;

public class CorrelationService {
    public static void main(String[] args) {
        KafkaManager kafkaManager = new KafkaManager();
        DisruptorManager disruptorManager = new DisruptorManager(kafkaManager, 1024);
    }
}
