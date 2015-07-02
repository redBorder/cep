package net.redborder.correlation.siddhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.List;
import java.util.Map;

public class ExecutionPlan {
    private final Logger log = LoggerFactory.getLogger(ExecutionPlan.class);
    private final List<String> inputTopics;
    private final Map<String, String> outputTopics;
    private final String plan, id;
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;

    public static ExecutionPlan fromMap(Map<String, Object> map) {
        List<String> inputTopics = (List<String>) map.get("input");
        Map<String, String> outputTopics = (Map<String, String>) map.get("output");
        String id = (String) map.get("id");
        String plan = (String) map.get("executionPlan");

        return new ExecutionPlan(id, inputTopics, outputTopics, plan);
    }

    public ExecutionPlan(String id, List<String> inputTopics,
                         Map<String, String> outputTopics, String plan) {
        this.id = id;
        this.inputTopics = inputTopics;
        this.outputTopics = outputTopics;
        this.plan = plan;
    }

    public void start(SiddhiManager siddhiManager) {
        this.siddhiManager = siddhiManager;
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(getPlan());

        if (executionPlanRuntime != null) {
            for (Map.Entry<String, String> entry : outputTopics.entrySet()) {
                final String streamName = entry.getKey();
                final List<Attribute> attributes = executionPlanRuntime.getStreamDefinitionMap().get(streamName).getAttributeList();

                executionPlanRuntime.addCallback(streamName, new QueryCallback() {
                    @Override
                    public void receive(long l, Event[] events, Event[] events1) {
                        for (Event event : events) {
                            System.out.println(event.getData());
                        }
                    }
                });
            }

            executionPlanRuntime.start();
            log.info("Starting execution plan with id {} and plan {}", id, getPlan());
        } else {
            log.error("Siddhi is not initialized!");
        }
    }

    public void stop() {
        this.executionPlanRuntime.shutdown();
        log.info("Stopped execution plan {}", id);
    }

    public InputHandler getInputHandler() {
        return executionPlanRuntime.getInputHandler("raw_rb_flow");
    }

    public String getPlan() {
        String executionPlan = "@config(async = 'true') define stream raw_rb_flow (src string, dst string, bytes int);";
        executionPlan += "from raw_rb_flow[namespace_id == 11111111] select src, dst, bytes insert into rb_flow;";
        executionPlan += this.plan;
        return executionPlan;
    }
}
