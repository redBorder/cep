package net.redborder.cep.senders;

import net.redborder.cep.sources.kafka.ProducerManager;

import java.util.Map;

public class KafkaSender implements EventSender {
    private ProducerManager producerManager;

    public KafkaSender(ProducerManager producerManager) {
        this.producerManager = producerManager;
    }

    @Override
    public void process(String executionPlanId, String streamName, String topic, Map<String, Object> message) {
        String clientMac = (String) message.get("client_mac");
        producerManager.send(topic, clientMac, message);
    }
}
