package net.redborder.correlation;

import net.redborder.correlation.kafka.ConsumerManager;
import net.redborder.correlation.kafka.ProducerManager;
import net.redborder.correlation.receivers.EventReceiver;
import net.redborder.correlation.receivers.KafkaReceiver;
import net.redborder.correlation.rest.RestManager;
import net.redborder.correlation.siddhi.SiddhiHandler;
import net.redborder.correlation.util.ConfigData;

public class CorrelationService {
    public static void main(String[] args) {
        // ProducerManager is in charge of emitting messages from this application to kafka
        // The KafkaReceiver wraps the producer manager in order to be used by siddhi handler
        ProducerManager producerManager = new ProducerManager();
        EventReceiver eventReceiver = new KafkaReceiver(producerManager);

        // Siddhi is in charge of processing the events coming from kafka, interpreting
        // the queries that comes from the REST API and correlating the events
        SiddhiHandler siddhiHandler = new SiddhiHandler(eventReceiver);

        // ConsumerManager coordinates the threads that consumes events from kafka
        // The messages read from kafka are sent to siddhi handler
        ConsumerManager consumerManager = new ConsumerManager(siddhiHandler);

        // RestManager starts the REST API and redirects the queries
        // that users add with it to SiddhiHandler.
        String restUri = ConfigData.getRESTURI();
        RestManager.startServer(restUri, siddhiHandler);
    }
}
