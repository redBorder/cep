package net.redborder.correlation.siddhi;

import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import net.redborder.correlation.siddhi.exceptions.TransformException;
import org.glassfish.grizzly.http.util.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
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
    private final String executionPlan, id;
    private final long version;
    private byte[] snapshot;
    private ExecutionPlanRuntime executionPlanRuntime;

    public static ExecutionPlan fromMap(Map<String, Object> map) throws ExecutionPlanException {
        String id = getField(map, "id", String.class);
        List<String> inputTopics = getField(map, "input", List.class);
        Map<String, String> outputTopics = getField(map, "output", Map.class);
        String plan = getField(map, "executionPlan", String.class);
        Integer version = getField(map, "version", 0, Integer.class);
        byte[] snapshot = getField(map, "snapshot", null, byte[].class);

        return new ExecutionPlan(id, version, inputTopics, outputTopics, plan, snapshot);
    }

    private static <T> T getField(Map<String, Object> map, String fieldName, T defaultValue, Class<T> type, boolean required) throws ExecutionPlanException {
        T result;

        try {
            Object fromMap = map.get(fieldName);
            result = (T) type.cast(fromMap);

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

    public ExecutionPlan(String id, Integer version, List<String> inputTopics,
                         Map<String, String> outputTopics,
                         String executionPlan, byte[] snapshot) {
        this.id = id;
        this.version = version;
        this.inputTopics = inputTopics;
        this.outputTopics = outputTopics;
        this.executionPlan = executionPlan;
        this.snapshot = snapshot;
    }

    public ExecutionPlan(String id, Integer version, List<String> inputTopics,
                         Map<String, String> outputTopics,
                         String executionPlan) {
        this(id, version, inputTopics, outputTopics, executionPlan, null);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("id", id);
        result.put("input", inputTopics);
        result.put("version", version);
        result.put("output", outputTopics);
        result.put("executionPlan", executionPlan);
        result.put("snapshot", snapshot);
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

    public String getFullExecutionPlan() {
        String executionPlan = "@config(async = 'true') define stream raw_rb_flow (src string, dst string, namespace_uuid string, bytes int, pkts int);";
        executionPlan += "from raw_rb_flow[namespace_uuid == '11111111'] select src, dst, bytes, pkts insert into rb_flow;";
        executionPlan += this.executionPlan;
        return executionPlan;
    }

    public byte[] getSnapshot() {
        return snapshot;
    }

    public void send(Object[] items) {
        try {
            executionPlanRuntime.getInputHandler("raw_rb_flow").send(items);
        } catch (InterruptedException e) {
            log.error("Couldn't send items to execution plan {}", id);
        }
    }

    public void updateSnapshot() {
        snapshot = executionPlanRuntime.snapshot();
    }

    public void start(SiddhiManager siddhiManager, SiddhiCallback siddhiCallback) {
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(getFullExecutionPlan());

        for (Map.Entry<String, String> entry : outputTopics.entrySet()) {
            String streamName = entry.getKey();
            String topic = entry.getValue();
            List<Attribute> attributes = executionPlanRuntime.getStreamDefinitionMap().get(streamName).getAttributeList();
            StreamCallback streamCallback = siddhiCallback.getCallback(getId(), streamName, topic, attributes);
            executionPlanRuntime.addCallback(streamName, streamCallback);
        }

        executionPlanRuntime.start();

        if (snapshot != null) {
            executionPlanRuntime.restore(snapshot);
            log.info("Started execution plan with snapshot {} id {} and plan {}", HexUtils.convert(snapshot), id, getExecutionPlan());
        } else {
            log.info("Started execution plan with id {} and plan {}", id, getExecutionPlan());
        }
    }

    public void stop() {
        this.executionPlanRuntime.shutdown();
        log.info("Stopped execution plan {}", id);
    }
}
