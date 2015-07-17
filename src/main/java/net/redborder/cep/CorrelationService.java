package net.redborder.cep;

import net.redborder.cep.rest.RestManager;
import net.redborder.cep.sinks.Sink;
import net.redborder.cep.sinks.kafka.KafkaSink;
import net.redborder.cep.siddhi.SiddhiHandler;
import net.redborder.cep.sources.SourcesManager;
import net.redborder.cep.sources.parsers.ParsersManager;
import net.redborder.cep.util.ConfigData;

public class CorrelationService {
    public static final String DEFAULT_CONFIG_FILE = "./conf/config.yml";

    public static void main(String[] args) {
        // First, initialize the config file
        if (args.length >= 1) ConfigData.setConfigFile(args[0]);
        else ConfigData.setConfigFile(DEFAULT_CONFIG_FILE);

        // The KafkaSink wraps the kafka producer in order to be used by siddhi handler
        final Sink eventReceiver = new KafkaSink();

        // Siddhi is in charge of processing the events coming from kafka, interpreting
        // the queries that comes from the REST API and correlating the events
        final SiddhiHandler siddhiHandler = new SiddhiHandler(eventReceiver);
        siddhiHandler.restore();

        // ParserManager prepare all parsers and create relations between parsers and sources.
        ParsersManager parsersManager = new ParsersManager();

        // SourcesManager coordinates the sources that consumes events from streams
        // The messages read from streams are sent to siddhi handler
        final SourcesManager sourcesManager = new SourcesManager(parsersManager, siddhiHandler);

        // RestManager starts the REST API and redirects the queries
        // that users add with it to SiddhiHandler.
        String restUri = ConfigData.getRESTURI();
        RestManager.startServer(restUri, siddhiHandler);

        // Add a hook to execute when the user wants to exit
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                siddhiHandler.stop();
                sourcesManager.shutdown();
                eventReceiver.shutdown();
                RestManager.stopServer();
            }
        });
    }
}
