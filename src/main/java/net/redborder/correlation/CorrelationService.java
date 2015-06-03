package net.redborder.correlation;

import net.redborder.correlation.disruptor.DisruptorManager;
import net.redborder.correlation.disruptor.EventProducer;
import net.redborder.correlation.kafka.KafkaManager;
import net.redborder.correlation.siddhi.RbSiddhiManager;
import net.redborder.correlation.util.ConfigFile;


public class CorrelationService {
    public static void main(String[] args) {
        RbSiddhiManager.init();
        DisruptorManager.init(1024);
        KafkaManager.init();
        ConfigFile.init();
    }
}
