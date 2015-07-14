package net.redborder.cep.receivers;

import net.redborder.cep.kafka.ProducerManager;

import java.util.Map;

public class KafkaReceiver implements EventReceiver {
    private ProducerManager producerManager;

    public KafkaReceiver(ProducerManager producerManager) {
        this.producerManager = producerManager;
    }

    @Override
    public void process(String executionPlanId, String streamName, String topic, Map<String, Object> message) {
        String clientMac = (String) message.get("client_mac");
        producerManager.send(topic, clientMac, message);
    }
}
