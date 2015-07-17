package net.redborder.cep;

import net.redborder.cep.receivers.kafka.ConsumerManager;
import net.redborder.cep.receivers.kafka.ProducerManager;
import net.redborder.cep.senders.EventSender;
import net.redborder.cep.senders.KafkaSender;
import net.redborder.cep.rest.RestManager;
import net.redborder.cep.siddhi.SiddhiHandler;
import net.redborder.cep.util.ConfigData;

public class CorrelationService {
    public static void main(String[] args) {
        // ProducerManager is in charge of emitting messages from this application to kafka
        // The KafkaReceiver wraps the producer manager in order to be used by siddhi handler
        ProducerManager producerManager = new ProducerManager();
        EventSender eventReceiver = new KafkaSender(producerManager);

        // Siddhi is in charge of processing the events coming from kafka, interpreting
        // the queries that comes from the REST API and correlating the events
        SiddhiHandler siddhiHandler = new SiddhiHandler(eventReceiver);
        siddhiHandler.restore();

        // ConsumerManager coordinates the threads that consumes events from kafka
        // The messages read from kafka are sent to siddhi handler
        ConsumerManager consumerManager = new ConsumerManager(siddhiHandler);

        // RestManager starts the REST API and redirects the queries
        // that users add with it to SiddhiHandler.
        String restUri = ConfigData.getRESTURI();
        RestManager.startServer(restUri, siddhiHandler);
    }
}
