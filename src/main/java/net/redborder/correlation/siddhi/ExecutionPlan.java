package net.redborder.correlation.siddhi;

import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import net.redborder.correlation.siddhi.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionPlan {
    private final Logger log = LoggerFactory.getLogger(ExecutionPlan.class);

    private final List<String> inputTopics;
    private final Map<String, String> outputTopics;
    private final String plan, id;
    private ExecutionPlanRuntime executionPlanRuntime;

    public static ExecutionPlan fromMap(Map<String, Object> map) throws ExecutionPlanException {
        List<String> inputTopics = getField(map, "input", List.class);
        Map<String, String> outputTopics = getField(map, "output", Map.class);
        String id = getField(map, "id", String.class);
        String plan = getField(map, "executionPlan", String.class);

        return new ExecutionPlan(id, inputTopics, outputTopics, plan);
    }

    private static <T> T getField(Map<String, Object> map, String fieldName, Class<T> type) throws ExecutionPlanException {
        T result;

        try {
            Object fromMap = map.get(fieldName);
            result = (T) type.cast(fromMap);

            if (result == null) {
                throw new TransformException("required field " + fieldName + " not found");
            }
        } catch (ClassCastException e) {
            throw new TransformException("invalid type for field " + fieldName, e);
        }

        return result;
    }

    public ExecutionPlan(String id, List<String> inputTopics,
                         Map<String, String> outputTopics, String plan) {
        this.id = id;
        this.inputTopics = inputTopics;
        this.outputTopics = outputTopics;
        this.plan = plan;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("input", inputTopics);
        result.put("output", outputTopics);
        result.put("executionPlan", plan);
        return result;
    }

    public List<String> getInputTopics() {
        return Collections.unmodifiableList(inputTopics);
    }

    public Map<String, String> getOutputTopics() {
        return Collections.unmodifiableMap(outputTopics);
    }

    public String getId() {
        return id;
    }

    public String getPlan() {
        return this.plan;
    }

    private String getFullPlan() {
        String executionPlan = "@config(async = 'true') define stream raw_rb_flow (src string, dst string, namespace_uuid string, bytes int);";
        executionPlan += "from raw_rb_flow[namespace_uuid == '11111111'] select src, dst, bytes insert into rb_flow;";
        executionPlan += this.plan;
        return executionPlan;
    }

    public void start(SiddhiManager siddhiManager, SiddhiCallback siddhiCallback) {
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(getFullPlan());

        for (Map.Entry<String, String> entry : outputTopics.entrySet()) {
            String streamName = entry.getKey();
            String topic = entry.getValue();
            List<Attribute> attributes = executionPlanRuntime.getStreamDefinitionMap().get(streamName).getAttributeList();
            StreamCallback streamCallback = siddhiCallback.getCallback(getId(), streamName, topic, attributes);
            executionPlanRuntime.addCallback(streamName, streamCallback);
        }

        executionPlanRuntime.start();
        log.info("Started execution plan with id {} and plan {}", id, getPlan());
    }

    public void stop() {
        this.executionPlanRuntime.shutdown();
        log.info("Stopped execution plan {}", id);
    }

    public InputHandler getInputHandler() {
        return executionPlanRuntime.getInputHandler("raw_rb_flow");
    }
}
