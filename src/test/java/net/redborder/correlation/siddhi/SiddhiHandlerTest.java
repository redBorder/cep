package net.redborder.correlation.siddhi;

import junit.framework.TestCase;
import net.redborder.correlation.kafka.disruptor.MapEvent;
import net.redborder.correlation.rest.exceptions.RestException;
import net.redborder.correlation.rest.exceptions.RestNotFoundException;
import net.redborder.correlation.siddhi.exceptions.ExecutionPlanException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SiddhiHandlerTest extends TestCase {
    @Test
    public void add() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

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
    public void addPresent() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

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

    @Test(expected=RestException.class)
    public void addInvalid() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select **ERROR** insert into testOutput");

        // Try to add the execution plan
        siddhiHandler.add(executionPlanMap);
    }

    @Test(expected=RestException.class)
    public void addInvalidMap() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");

        // Try to add the execution plan
        siddhiHandler.add(executionPlanMap);
    }

    @Test
    public void remove() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

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

        // Remove it
        siddhiHandler.remove("testID");

        assertTrue(siddhiHandler.getExecutionPlans().isEmpty());
    }

    @Test(expected = RestNotFoundException.class)
    public void removeNotPresent() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Remove it
        siddhiHandler.remove("testID");
    }

    @Test
    public void synchronizes() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "rb_alert");
        executionPlanMap2.put("id", "testID2");
        executionPlanMap2.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from rb_flow select bytes insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);

        // Check that both have been added
        assertEquals(2, siddhiHandler.getExecutionPlans().size());
    }

    @Test(expected = RestException.class)
    public void synchronizeRepeatedID() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "rb_alert");
        executionPlanMap2.put("id", "testID");
        executionPlanMap2.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from rb_flow select bytes insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);
    }

    @Test(expected = RestException.class)
    public void synchronizeInvalidMap() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "rb_alert");
        executionPlanMap2.put("id", "testID");
        // executionPlanMap2.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from rb_flow select bytes insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);
    }

    @Test(expected = RestException.class)
    public void synchronizeInvalid() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select **ERROR** insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);
    }

    @Test
    public void list() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "rb_alert");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from rb_flow select src insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "rb_alert");
        executionPlanMap2.put("id", "testID2");
        executionPlanMap2.put("input", Arrays.asList("rb_flow", "rb_event"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from rb_flow select bytes insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);

        // Check that both lists are equal
        assertTrue(executionPlans.containsAll(siddhiHandler.list()));
        assertTrue(siddhiHandler.list().containsAll(executionPlans));
    }

    @Test
    public void stop() throws ExecutionPlanException {
        // Create two mocks
        ExecutionPlan executionPlanMock = mock(ExecutionPlan.class);
        when(executionPlanMock.getId()).thenReturn("testMockID");
        ExecutionPlan executionPlanMockTwo = mock(ExecutionPlan.class);
        when(executionPlanMockTwo.getId()).thenReturn("testMockIDTwo");

        // Create the handler and add both execution plans
        SiddhiHandler siddhiHandler = new SiddhiHandler();
        siddhiHandler.add(executionPlanMock);
        siddhiHandler.add(executionPlanMockTwo);

        // Call stop
        siddhiHandler.stop();

        // Now verify that the stop method was called
        verify(executionPlanMock).stop();
        verify(executionPlanMockTwo).stop();
    }

    @Test
    public void onEvent() throws Exception {
        // Create mocks
        ExecutionPlan executionPlanMock = mock(ExecutionPlan.class);
        when(executionPlanMock.getId()).thenReturn("testMockID");
        InputHandler inputHandlerMock = mock(InputHandler.class);
        when(executionPlanMock.getInputHandler()).thenReturn(inputHandlerMock);

        // Create the handler and add the execution plan mock
        SiddhiHandler siddhiHandler = new SiddhiHandler();
        siddhiHandler.add(executionPlanMock);

        // Create the event map
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("src", "1.1.1.1");
        dataMap.put("dst", "2.2.2.2");
        dataMap.put("namespace_uuid", "11111111");
        dataMap.put("bytes", 128);
        MapEvent mapEventStub = new MapEvent();
        mapEventStub.setData(dataMap);

        // Send the event
        siddhiHandler.onEvent(mapEventStub, 1, true);

        // Checks if send was called
        verify(inputHandlerMock).send(new Object[] { "1.1.1.1", "2.2.2.2", "11111111", 128 });
    }
}
