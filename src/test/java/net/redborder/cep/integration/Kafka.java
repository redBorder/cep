package net.redborder.cep.integration;


import junit.framework.TestCase;
import net.redborder.cep.rest.exceptions.RestException;
import net.redborder.cep.siddhi.SiddhiHandler;
import net.redborder.cep.siddhi.SiddhiHandlerTest;
import net.redborder.cep.sinks.SinksManager;
import net.redborder.cep.util.ConfigFile;
import org.apache.kafka.streams.KeyValue;
import net.redborder.cep.sources.SourcesManager;
import net.redborder.cep.sources.parsers.ParsersManager;
import net.redborder.cep.util.ConfigData;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@RunWith(MockitoJUnitRunner.class)
public class Kafka extends TestCase {

    @ClassRule
    public static EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "test";

    private static final String OUTPUT_TOPIC = "test_output";

    private static KafkaProducer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;
    private static Properties producerProps;
    private static Properties consumerProps;

    public static void startKafkaCluster() throws Exception {

        // inputs
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);

        // sinks
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.start();
    }

    public static void initConfig() {

        URL testConfigPath = SiddhiHandlerTest.class.getClassLoader().getResource("config.yml");
        ConfigData.setConfigFile(testConfigPath.getFile());

        Map<String, Object> mappedConfig = new HashMap<>();

        List<Map<String, Object>> sinksList = new LinkedList<>();
        Map<String, Object> sinksMap = new HashMap<>();
        sinksMap.put("name", "kafka");
        sinksMap.put("class", "net.redborder.cep.sinks.kafka.KafkaSink");
        Map<String, Object> kafkaBrokers = new HashMap<>();
        kafkaBrokers.put("kafka_brokers", CLUSTER.bootstrapServers());
        sinksMap.put("properties", kafkaBrokers);
        sinksList.add(sinksMap);

        Map<String, Object> parsersMap = new HashMap<>();
        parsersMap.put("json", "net.redborder.cep.sources.parsers.JsonParser");

        Map<String, Object> testStream = new HashMap<>();
        Map<String, Object> testStreamProperties = new HashMap<>();
        testStreamProperties.put("source", "kafka");
        Map<String, Object> testStreamAttributes = new HashMap<>();
        testStreamAttributes.put("d", "int");
        testStreamAttributes.put("e", "int");
        testStreamAttributes.put("b", "string");
        testStreamAttributes.put("c", "string");
        testStreamAttributes.put("a", "string");
        testStreamProperties.put("attributes", testStreamAttributes);
        testStreamProperties.put("parser", "json");
        testStream.put("test", testStreamProperties);

        List<Map<String, Object>> sourcesList = new LinkedList<>();
        Map<String, Object> sourcesMap = new HashMap<>();
        sourcesMap.put("name", "kafka");
        sourcesMap.put("class", "net.redborder.cep.sources.kafka.KafkaSource");
        kafkaBrokers = new HashMap<>();
        kafkaBrokers.put("zk_connect", CLUSTER.zKConnectString());
        kafkaBrokers.put("kafka_brokers", CLUSTER.bootstrapServers());
        sourcesMap.put("properties", kafkaBrokers);
        sourcesList.add(sourcesMap);

        mappedConfig.put("sinks", sinksList);
        mappedConfig.put("sources", sourcesList);
        mappedConfig.put("parsers", parsersMap);
        mappedConfig.put("streams", testStream);
        mappedConfig.put("rest_uri", "http://127.0.0.1:8888");
        mappedConfig.put("state_file", "/tmp/testState.json");

        //Config used at Consumer class in order to read from beggining
        mappedConfig.put("kafka_offset", "earliest");

        ConfigFile configFile = new ConfigFile(mappedConfig);

        ConfigData.setConfigFile(configFile);

    }

    @BeforeClass
    public static void init() throws Exception {
        startKafkaCluster();
        initConfig();
        // The producer config attributes
        producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(PARTITIONER_CLASS_CONFIG, "net.redborder.cep.sinks.kafka.SimplePartitioner");
        producerProps.put(ACKS_CONFIG, "all");


        consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        consumerProps.put(GROUP_ID_CONFIG, "test");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Initialize the producer
        producer = new KafkaProducer<String, String>(producerProps);

        //Initialize the consumer
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        });

        // The KafkaSink wraps the kafka producer in order to be used by siddhi handler
        final SinksManager sinksManager = new SinksManager();

        // Siddhi is in charge of processing the events coming from kafka, interpreting
        // the queries that comes from the REST API and correlating the events
        final SiddhiHandler siddhiHandler = new SiddhiHandler(sinksManager);

        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", OUTPUT_TOPIC);
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList(INPUT_TOPIC));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from " + INPUT_TOPIC + " select a insert into testOutput");

        // Add it to siddhi handler
        try {
            siddhiHandler.add(executionPlanMap);
        } catch (RestException e) {
            e.printStackTrace();
        }


        // ParserManager prepare all parsers and create relations between parsers and sources.
        ParsersManager parsersManager = new ParsersManager();

        // SourcesManager coordinates the sources that consumes events from streams
        // The messages read from streams are sent to siddhi handler
        final SourcesManager sourcesManager = new SourcesManager(parsersManager, siddhiHandler);

    }

    @Test
    public void sendKeyedMessages() throws RestException {

        KeyValue<String, String> keyedMessage = new KeyValue<>("keyA", "{\"a\":\"test\"}");

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(keyedMessage), producerProps);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        KeyValue<String, String> expectedDataKv = new KeyValue<>("keyA", "{\"a\":\"test\"}");

        try {
            List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProps, OUTPUT_TOPIC, 1);
            assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void sendUnkeyedMessages() throws RestException {

        KeyValue<String, String> keyedMessage = new KeyValue<>(null, "{\"a\":\"test\"}");

        try {
            IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(keyedMessage), producerProps);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        KeyValue<String, String> expectedDataKv = new KeyValue<>(null, "{\"a\":\"test\"}");

        try {
            List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerProps, OUTPUT_TOPIC, 1);
            assertEquals(Collections.singletonList(expectedDataKv), receivedMessagesFromOutput);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
