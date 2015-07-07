package net.redborder.correlation.siddhi;

import junit.framework.TestCase;
import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import net.redborder.correlation.siddhi.exceptions.TransformException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionPlanTest extends TestCase {
    @Test
    public void creates() {
        // Execution plan data
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        List<String> inputTopics = Arrays.asList("rb_flow", "rb_event");
        String id = "testID";
        String plan = "from rb_flow select src insert into testOutput";

        // Create the execution plan
        ExecutionPlan executionPlan = new ExecutionPlan(id, inputTopics, outputTopics, plan);

        // Check it values
        assertEquals(id, executionPlan.getId());
        assertEquals(inputTopics, executionPlan.getInputTopics());
        assertEquals(outputTopics, executionPlan.getOutputTopics());
        assertEquals(plan, executionPlan.getPlan());
    }

    @Test
    public void createsFromMap() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");

        // Create the execution plan
        ExecutionPlan executionPlan = ExecutionPlan.fromMap(executionPlanMap);

        assertEquals(executionPlanMap, executionPlan.toMap());
    }

    @Test(expected = TransformException.class)
    public void createsFromMapInvalid() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");

        // Create the execution plan
        ExecutionPlan.fromMap(executionPlanMap);
    }

    @Test(expected = TransformException.class)
    public void createsFromMapInvalidTypes() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "invalid_rule");
        executionPlanMap.put("input", 2); // INVALID
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");

        // Create the execution plan
        ExecutionPlan.fromMap(executionPlanMap);
    }
}
