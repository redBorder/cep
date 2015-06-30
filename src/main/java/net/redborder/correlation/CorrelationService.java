package net.redborder.correlation;

import net.redborder.correlation.kafka.KafkaManager;
import net.redborder.correlation.rest.RestManager;
import net.redborder.correlation.siddhi.SiddhiHandler;

public class CorrelationService {
    public static void main(String[] args) {
        // Siddhi is in charge of processing the events coming from kafka, interpreting
        // the queries that comes from the REST API and correlating the events
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // KafkaManager coordinates the threads that consumes events from kafka
        // The messages read from kafka are sent to siddhi handler
        KafkaManager kafkaManager = new KafkaManager(siddhiHandler);

        // RestManager starts the REST API and redirects the queries
        // that users add with it to SiddhiHandler.
        RestManager.startServer(siddhiHandler);
    }
}
