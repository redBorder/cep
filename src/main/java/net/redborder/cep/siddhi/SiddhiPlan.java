package net.redborder.cep.siddhi;

import com.google.common.base.Joiner;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.InvalidExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.TransformException;
import net.redborder.cep.util.ConfigData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.*;

/**
 * This class represents the unit of work of the CEP.
 * Each SiddhiPlan is composed by a Siddhi execution plan, and some metadata that
 * allows the CEP to connect the execution plan to kafka on both input and output messages.
 */

public class SiddhiPlan {
    private final static Logger log = LogManager.getLogger(SiddhiPlan.class);

    // The list of streams definitions for every input stream defined on the config file
    private final static Map<String, StreamDefinition> streamDefinitions = new HashMap<>();

    // Get the list of available streams from the config file
    // Also, get the Siddhi representation of this streams with their attributes
    static {
        Set<String> topics = ConfigData.getStreams();
        for (String topic : topics) {
            StreamDefinition stream = new StreamDefinition();
            stream.setId(topic);

            log.info("Creating stream definition for topic {}", topic);
            Map<String, String> attributes = ConfigData.getAttributes(topic);
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String name = entry.getKey();
                String type = entry.getValue();
                Attribute.Type attrType = SiddhiUtils.typeOf(type);

                if (attrType != null) {
                    stream.attribute(name, attrType);
                    log.debug("  - {} : {}", name, type);
                } else {
                    log.warn("  - Incorrect type for attribute {}", name);
                }
            }

            streamDefinitions.put(topic, stream);
        }
    }

    // The list of topics that this plan will use as input
    private final List<String> inputTopics;

    // A map that relates a siddhi output stream to a output topic
    private final Map<String, String> outputTopics;

    // A list of filters that will be used on the input topics
    private final Map<String, String> filters;

    // The user-provided execution plan and id
    private final String executionPlan, id;

    // The user-provided execution plan with the stream definitions appended
    private final String fullExecutionPlan;

    // The version of the plan
    private final int version;

    // The execution plan runtime that lets you start and stop the execution plan
    private ExecutionPlanRuntime executionPlanRuntime;

    /**
     * This method creates a new SiddhiPlan instance from a given map
     *
     * @param map The map representation of a SiddhiPlan
     * @return A new SiddhiPlan instance with the data provided as a map
     * @throws ExecutionPlanException if the map provided does not follow the siddhi plan schema
     */

    @SuppressWarnings("unchecked")
    public static SiddhiPlan fromMap(Map<String, Object> map) throws ExecutionPlanException {
        String id = getField(map, "id", String.class);
        List<String> inputTopics = getField(map, "input", List.class);
        Map<String, String> outputTopics = getField(map, "output", Map.class);
        Map<String, String> filters = getField(map, "filters", Collections.emptyMap(), Map.class);
        String plan = getField(map, "executionPlan", String.class);
        Integer version = getField(map, "version", 0, Integer.class);

        return new SiddhiPlan(id, version, inputTopics, outputTopics, filters, plan);
    }

    /**
     * This method gets the value associated with a key from a map, casting it to the provided class.
     *
     * @param map          The map where will try to get the value associated with the key
     * @param key          The key that represents the value that you want to get
     * @param defaultValue A default value in case the key is not present in the map
     * @param type         The type of the value
     * @param required     Specifies if the value is optional or required
     * @return An object with the value extracted from the map
     * @throws ExecutionPlanException if the value couldn't be casted to the class provided, or if the key was not
     *                                present and the value was required.
     */

    private static <T> T getField(Map<String, Object> map, String key, T defaultValue, Class<T> type, boolean required) throws ExecutionPlanException {
        T result;

        try {
            Object fromMap = map.get(key);
            result = type.cast(fromMap);

            if (result == null) {
                if (!required) {
                    result = defaultValue;
                } else {
                    throw new TransformException("required field " + key + " not found");
                }
            }
        } catch (ClassCastException e) {
            throw new TransformException("invalid type for field " + key, e);
        }

        return result;
    }

    /**
     * Gets a required value from map associated with the key given
     *
     * @param map  The map where will try to get the value associated with the key
     * @param key  The key that represents the value that you want to get
     * @param type The type of the value
     * @return An object with the value extracted from the map
     * @throws ExecutionPlanException if the value couldn't be casted to the class provided, or if the key was not
     *                                present
     */

    private static <T> T getField(Map<String, Object> map, String key, Class<T> type) throws ExecutionPlanException {
        return getField(map, key, null, type, true);
    }

    /**
     * Gets an optional value from map associated with the key given or the default value if the key is not present
     *
     * @param map          The map where will try to get the value associated with the key
     * @param key          The key that represents the value that you want to get
     * @param defaultValue A default value in case the key is not present in the map
     * @param type         The type of the value
     * @return An object with the value extracted from the map
     * @throws ExecutionPlanException if the value couldn't be casted to the class provided, or if the key was not
     *                                present
     */

    private static <T> T getField(Map<String, Object> map, String key, T defaultValue, Class<T> type) throws ExecutionPlanException {
        return getField(map, key, defaultValue, type, false);
    }

    /**
     * Creates a new Siddhi plan from parameters
     *
     * @param id            The unique ID
     * @param version       The version
     * @param inputTopics   List of input streams
     * @param outputTopics  Mapping from a Siddhi stream to an output
     * @param filters       A list of filters as a map
     * @param executionPlan The user-provided execution plan
     * @throws ExecutionPlanException if the execution plan is invalid
     */

    public SiddhiPlan(String id, int version, List<String> inputTopics,
                      Map<String, String> outputTopics, Map<String, String> filters,
                      String executionPlan) throws ExecutionPlanException {
        this.id = id;
        this.version = version;
        this.inputTopics = inputTopics;
        this.outputTopics = outputTopics;
        this.filters = filters;
        this.executionPlan = executionPlan;
        this.fullExecutionPlan = build();
    }

    /**
     * Transforms the siddhi plan into its map representation.
     * This representation can later be passed to the #fromMap method to create the same
     * siddhi plan again.
     *
     * @return A map representation of the siddhi plan
     */

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("input", inputTopics);
        result.put("version", version);
        result.put("output", outputTopics);
        result.put("filters", filters);
        result.put("executionPlan", executionPlan);
        return result;
    }

    /**
     * @return the id of the siddhi plan
     */

    public String getId() {
        return id;
    }

    /**
     * @return the version of the siddhi plan
     */

    public long getVersion() {
        return version;
    }

    /**
     * @return the list of inputs of the siddhi plan
     */

    public List<String> getInput() {
        return Collections.unmodifiableList(inputTopics);
    }

    /**
     * @return the outputs of the siddhi plan
     */

    public Map<String, String> getOutput() {
        return Collections.unmodifiableMap(outputTopics);
    }

    /**
     * @return the user-provided execution plan of the siddhi plan
     */

    public String getExecutionPlan() {
        return this.executionPlan;
    }

    /**
     * Builds the execution plan from the input topics and the user-provided execution plan
     *
     * @return A string with the full siddhi-formatted execution plan
     * @throws ExecutionPlanException if any input specified is not found
     */

    public String build() throws ExecutionPlanException {
        StringBuilder fullExecutionPlanBuilder = new StringBuilder();
        StringBuilder streamsWithFiltersBuilder = new StringBuilder();

        for (String topic : inputTopics) {
            StreamDefinition streamDefinition = streamDefinitions.get(topic);

            if (streamDefinition == null) {
                throw new InvalidExecutionPlanException("No stream definition found for topic " + topic + " on execution plan id " + id + "!");
            }

            List<Attribute> attributes = streamDefinition.getAttributeList();
            List<String> attributesWithType = new ArrayList<>();
            List<String> attributesNameList = new ArrayList<>();

            for (Attribute attribute : attributes) {
                attributesWithType.add(attribute.getName() + " " + attribute.getType());
                attributesNameList.add(attribute.getName());
            }

            String attributesWithTypeList = Joiner.on(", ").join(attributesWithType);

            fullExecutionPlanBuilder.append("define stream raw_")
                    .append(topic)
                    .append(" (")
                    .append(attributesWithTypeList)
                    .append(");");

            List<String> filtersList = new ArrayList<>();
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                String field = entry.getKey();
                String value = entry.getValue();
                filtersList.add(field + " == '" + value + "'");
            }

            if (filtersList.isEmpty()) {
                streamsWithFiltersBuilder.append("from raw_")
                        .append(topic);
            } else {
                String filtersString = Joiner.on(" and ").join(filtersList);

                streamsWithFiltersBuilder.append("from raw_")
                        .append(topic)
                        .append("[")
                        .append(filtersString)
                        .append("]");
            }

            String attributesString = Joiner.on(", ").join(attributesNameList);

            streamsWithFiltersBuilder.append(" select ")
                    .append(attributesString)
                    .append(" insert into ")
                    .append(topic)
                    .append(";");
        }

        fullExecutionPlanBuilder.append(streamsWithFiltersBuilder);

        //Add __KEY to the executionPlan in order to pass the kafka key through Siddhi Engine
        StringBuilder executionPlanWithKey = new StringBuilder();
        if (executionPlan.toLowerCase().contains("select ")) {
            int index = executionPlan.toLowerCase().indexOf("select ");
            executionPlanWithKey.append(executionPlan.substring(0, index + 7));
            executionPlanWithKey.append("__KEY, ");
            executionPlanWithKey.append(executionPlan.substring(index + 7));
        }

        fullExecutionPlanBuilder.append(executionPlanWithKey.toString());
        return fullExecutionPlanBuilder.toString();
    }

    /**
     * @return the full execution plan
     * @see #build()
     */

    public String getFullExecutionPlan() {
        return fullExecutionPlan;
    }

    /**
     * Process a map as a input message for the execution plan runtime
     *
     * @param topicName The message's stream input name
     * @param data      The message
     */

    public void send(String topicName, Map<String, Object> data) {
        if (!inputTopics.contains(topicName)) {
            log.warn("Received send from invalid topic {} to execution plan {} version {}", topicName, id, version);
            return;
        }

        List<Object> elementsList = new ArrayList<>();
        Map<String, String> attributes = ConfigData.getAttributes(topicName);
        for (String attributeName : attributes.keySet()) {
            Object element = data.get(attributeName);
            elementsList.add(element);
        }

        try {
            executionPlanRuntime.getInputHandler("raw_" + topicName).send(elementsList.toArray());
        } catch (InterruptedException e) {
            log.error("Couldn't send items to execution plan {} version {}", id, version);
        }
    }

    /**
     * Starts the siddhi execution plan
     *
     * @param siddhiManager  The manager that will manage the execution plan
     * @param siddhiCallback The callback to be called when the execution plan creates a new message
     * @throws ExecutionPlanException
     */

    public void start(SiddhiManager siddhiManager, SiddhiCallback siddhiCallback) throws ExecutionPlanException {
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(getFullExecutionPlan());

        for (Map.Entry<String, String> entry : outputTopics.entrySet()) {
            String streamName = entry.getKey();
            String topic = entry.getValue();

            AbstractDefinition abstractDefinition = executionPlanRuntime.getStreamDefinitionMap().get(streamName);
            if (abstractDefinition != null) {
                List<Attribute> attributes = abstractDefinition.getAttributeList();
                StreamCallback streamCallback = siddhiCallback.getCallback(streamName, topic, attributes);
                executionPlanRuntime.addCallback(streamName, streamCallback);
            } else {
                throw new InvalidExecutionPlanException("You specified a output that is not present on the execution plan");
            }
        }

        executionPlanRuntime.start();
        log.info("Started execution plan with id {} version {}", id, version, fullExecutionPlan);
    }

    /**
     * Stops the execution plan
     */

    public void stop() {
        this.executionPlanRuntime.shutdown();
        log.info("Stopped execution plan {} version {}", id, version);
    }
}
