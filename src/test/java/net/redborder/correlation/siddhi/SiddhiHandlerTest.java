package net.redborder.correlation.siddhi;

import junit.framework.TestCase;
import net.redborder.correlation.rest.RestException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SiddhiHandlerTest extends TestCase {
    static SiddhiHandler siddhiHandler = new SiddhiHandler();

    @Test
    public void addsAnExecutionPlan() throws RestException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");

        // Add it to siddhi handler
        siddhiHandler.add(executionPlanMap);

        // Assert that the execution plan was added
        assertTrue(siddhiHandler.getExecutionPlans().containsKey("testID"));
    }

    @Test(expected=RestException.class)
    public void cantAddAnExecutionPlanWithUsedID() throws RestException {
        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");
        siddhiHandler.add(executionPlanMap);

        // Try to add it again
        siddhiHandler.add(executionPlanMap);
    }
}
