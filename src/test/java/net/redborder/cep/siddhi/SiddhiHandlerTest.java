package net.redborder.cep.siddhi;

import junit.framework.TestCase;
import net.redborder.cep.sources.disruptor.MapEvent;
import net.redborder.cep.rest.exceptions.RestException;
import net.redborder.cep.rest.exceptions.RestNotFoundException;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.util.ConfigData;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URL;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SiddhiHandlerTest extends TestCase {
    @BeforeClass
    public static void init() {
        URL testConfigPath = SiddhiHandlerTest.class.getClassLoader().getResource("config.yml");
        ConfigData.setConfigFile(testConfigPath.getFile());
    }

    @Test
    public void add() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Add it to siddhi handler
        siddhiHandler.add(executionPlanMap);

        // Assert that the execution plan was added
        assertTrue(siddhiHandler.getSiddhiPlans().containsKey("testID"));
    }

    @Test(expected = RestException.class)
    public void addPresent() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        siddhiHandler.add(executionPlanMap);

        // Try to add it again
        siddhiHandler.add(executionPlanMap);
    }

    @Test
    public void addPresentWithHigherVersion() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        siddhiHandler.add(executionPlanMap);

        // Try to add it again
        executionPlanMap.put("version", 1);
        siddhiHandler.add(executionPlanMap);
    }

    @Test(expected = RestException.class)
    public void addInvalid() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select **ERROR** insert into testOutput");

        // Try to add the execution plan
        siddhiHandler.add(executionPlanMap);
    }

    @Test(expected = RestException.class)
    public void addInvalidMap() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Add an execution plan with id testID
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Try to add the execution plan
        siddhiHandler.add(executionPlanMap);
    }

    @Test
    public void remove() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Add it to siddhi handler
        siddhiHandler.add(executionPlanMap);

        // Remove it
        siddhiHandler.remove("testID");

        assertTrue(siddhiHandler.getSiddhiPlans().isEmpty());
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
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "outputTopic");
        executionPlanMap2.put("id", "testID2");
        executionPlanMap2.put("input", Arrays.asList("test", "test2"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from test select d insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);

        // Check that both have been added
        assertEquals(2, siddhiHandler.getSiddhiPlans().size());
    }

    @Test
    public void synchronizeRemoveNotUsed() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "outputTopic");
        executionPlanMap2.put("id", "testID2");
        executionPlanMap2.put("input", Arrays.asList("test", "test2"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from test select d insert into testOutput");
        executionPlans.add(executionPlanMap2);

        // Build third execution plan
        Map<String, Object> executionPlanMap3 = new HashMap<>();
        Map<String, String> outputTopics3 = new HashMap<>();
        outputTopics3.put("testOutput", "outputTopic");
        executionPlanMap3.put("id", "testID3");
        executionPlanMap3.put("input", Arrays.asList("test", "test2"));
        executionPlanMap3.put("output", outputTopics3);
        executionPlanMap3.put("executionPlan", "from test select d insert into testOutput");
        executionPlans.add(executionPlanMap3);

        // Add both to siddhiHandler
        siddhiHandler.synchronize(executionPlans);

        // Build the list of execution plans
        executionPlans = new ArrayList<>();

        // Now add three more execution plans
        executionPlanMap2.put("id", "testID4");
        executionPlanMap3.put("version", 1);
        executionPlans.add(executionPlanMap);
        executionPlans.add(executionPlanMap2);
        executionPlans.add(executionPlanMap3);

        // Synchronize again
        siddhiHandler.synchronize(executionPlans);

        // The expected set of execution plans ids
        Set<String> expectedIds = new HashSet<>();
        expectedIds.add("testID");
        expectedIds.add("testID3");
        expectedIds.add("testID4");

        // Check that both have been added
        assertEquals(expectedIds, siddhiHandler.getSiddhiPlans().keySet());
    }

    @Test(expected = RestException.class)
    public void synchronizeRepeatedID() throws RestException {
        SiddhiHandler siddhiHandler = new SiddhiHandler();

        // Build the list of execution plans
        List<Map<String, Object>> executionPlans = new ArrayList<>();

        // Build the first execution plan
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "outputTopic");
        executionPlanMap2.put("id", "testID");
        executionPlanMap2.put("input", Arrays.asList("test", "test2"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from test select d insert into testOutput");
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
        outputTopics.put("testOutput", "outputTopic");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "outputTopic");
        executionPlanMap2.put("id", "testID");
        // executionPlanMap2.put("input", Arrays.asList("test", "test2"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from test select d insert into testOutput");
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
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select **ERROR** insert into testOutput");
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
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");
        executionPlanMap.put("version", 0);
        executionPlanMap.put("filters", Collections.emptyMap());
        executionPlans.add(executionPlanMap);

        // Build the second execution plan
        Map<String, Object> executionPlanMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("testOutput", "outputTopic");
        executionPlanMap2.put("id", "testID2");
        executionPlanMap2.put("input", Arrays.asList("test", "test2"));
        executionPlanMap2.put("output", outputTopics2);
        executionPlanMap2.put("executionPlan", "from test select d insert into testOutput");
        executionPlanMap2.put("version", 0);
        executionPlanMap2.put("filters", Collections.emptyMap());
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
        SiddhiPlan siddhiPlanMock = mock(SiddhiPlan.class);
        when(siddhiPlanMock.getId()).thenReturn("testMockID");
        SiddhiPlan siddhiPlanMockTwo = mock(SiddhiPlan.class);
        when(siddhiPlanMockTwo.getId()).thenReturn("testMockIDTwo");

        // Create the handler and add both execution plans
        SiddhiHandler siddhiHandler = new SiddhiHandler();
        siddhiHandler.add(siddhiPlanMock);
        siddhiHandler.add(siddhiPlanMockTwo);

        // Call stop
        siddhiHandler.stop();

        // Now verify that the stop method was called
        verify(siddhiPlanMock).stop();
        verify(siddhiPlanMockTwo).stop();
    }

    @Test
    public void onEvent() throws Exception {
        // Create mocks
        SiddhiPlan siddhiPlanMock = mock(SiddhiPlan.class);
        when(siddhiPlanMock.getId()).thenReturn("testMockID");
        when(siddhiPlanMock.getInput()).thenReturn(Collections.singletonList("test"));

        // Create the handler and add the execution plan mock
        SiddhiHandler siddhiHandler = new SiddhiHandler();
        siddhiHandler.add(siddhiPlanMock);

        // Create the event map
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("a", "testa");
        dataMap.put("b", "testb");
        dataMap.put("c", "testc");
        dataMap.put("d", 128);
        dataMap.put("e", 10);
        MapEvent mapEventStub = new MapEvent();
        mapEventStub.setData(dataMap);
        mapEventStub.setSource("test");

        // Send the event
        siddhiHandler.onEvent(mapEventStub, 1, true);

        // Checks if send was called
        verify(siddhiPlanMock).send("test", dataMap);
    }
}
