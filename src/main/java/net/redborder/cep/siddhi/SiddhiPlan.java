package net.redborder.cep.siddhi;

import com.google.common.base.Joiner;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.InvalidExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.TransformException;
import net.redborder.cep.util.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.*;

public class SiddhiPlan {
    private final static Logger log = LoggerFactory.getLogger(SiddhiPlan.class);
    private final static Map<String, StreamDefinition> streamDefinitions = new HashMap<>();

    // Get the list of available streams from the config file
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

    private final List<String> inputTopics;
    private final Map<String, String> outputTopics;
    private final Map<String, String> filters;
    private final String executionPlan, id;
    private final String fullExecutionPlan;
    private final int version;

    private ExecutionPlanRuntime executionPlanRuntime;

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

    private static <T> T getField(Map<String, Object> map, String fieldName, T defaultValue, Class<T> type, boolean required) throws ExecutionPlanException {
        T result;

        try {
            Object fromMap = map.get(fieldName);
            result = type.cast(fromMap);

            if (result == null) {
                if (!required) {
                    result = defaultValue;
                } else {
                    throw new TransformException("required field " + fieldName + " not found");
                }
            }
        } catch (ClassCastException e) {
            throw new TransformException("invalid type for field " + fieldName, e);
        }

        return result;
    }

    private static <T> T getField(Map<String, Object> map, String fieldName, Class<T> type) throws ExecutionPlanException {
        return getField(map, fieldName, null, type, true);
    }

    private static <T> T getField(Map<String, Object> map, String fieldName, T defaultValue, Class<T> type) throws ExecutionPlanException {
        return getField(map, fieldName, defaultValue, type, false);
    }

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

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public List<String> getInput() {
        return Collections.unmodifiableList(inputTopics);
    }

    public Map<String, String> getOutput() {
        return Collections.unmodifiableMap(outputTopics);
    }

    public String getExecutionPlan() {
        return this.executionPlan;
    }

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
        fullExecutionPlanBuilder.append(executionPlan);

        return fullExecutionPlanBuilder.toString();
    }

    public String getFullExecutionPlan() {
        return fullExecutionPlan;
    }

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

    public void stop() {
        this.executionPlanRuntime.shutdown();
        log.info("Stopped execution plan {} version {}", id, version);
    }
}
