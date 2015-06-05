package net.redborder.correlation;

import net.redborder.correlation.disruptor.DisruptorManager;
import net.redborder.correlation.kafka.KafkaManager;
import net.redborder.correlation.siddhi.RbSiddhiManager;

public class CorrelationService {
    public static void main(String[] args) {
        RbSiddhiManager.init();
        DisruptorManager.init(1024);
        KafkaManager.init();
    }
}
