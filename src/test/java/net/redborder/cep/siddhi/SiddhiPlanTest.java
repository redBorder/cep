package net.redborder.cep.siddhi;

import junit.framework.TestCase;
import net.redborder.cep.siddhi.exceptions.ExecutionPlanException;
import net.redborder.cep.siddhi.exceptions.TransformException;
import net.redborder.cep.util.ConfigData;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URL;
import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class SiddhiPlanTest extends TestCase {
    @BeforeClass
    public static void init() {
        URL testConfigPath = SiddhiHandlerTest.class.getClassLoader().getResource("config.yml");
        ConfigData.setConfigFile(testConfigPath.getFile());
    }

    @Test
    public void creates() throws ExecutionPlanException {
        // Execution plan data
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        Map<String, String> emptyFilters = Collections.emptyMap();
        List<String> inputTopics = Arrays.asList("test", "test2");
        String plan = "from test select a insert into testOutput";
        String id = "testID";
        int version = 0;

        // Create the execution plan
        SiddhiPlan siddhiPlan = new SiddhiPlan(id, 0, inputTopics, outputTopics, emptyFilters, plan);

        // Check it values
        assertEquals(id, siddhiPlan.getId());
        assertEquals(version, siddhiPlan.getVersion());
        assertEquals(inputTopics, siddhiPlan.getInput());
        assertEquals(outputTopics, siddhiPlan.getOutput());
        assertEquals(plan, siddhiPlan.getExecutionPlan());
    }

    @Test
    public void createsFromMap() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Create the execution plan
        SiddhiPlan siddhiPlan = SiddhiPlan.fromMap(executionPlanMap);
        executionPlanMap.put("version", 0);
        executionPlanMap.put("filters", Collections.emptyMap());

        assertEquals(executionPlanMap, siddhiPlan.toMap());
    }

    @Test(expected = TransformException.class)
    public void createsFromMapInvalid() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        // executionPlanMap.put("id", "testID");
        executionPlanMap.put("input", Arrays.asList("test", "test2"));
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Create the execution plan
        SiddhiPlan.fromMap(executionPlanMap);
    }

    @Test(expected = TransformException.class)
    public void createsFromMapInvalidTypes() throws ExecutionPlanException {
        // Build a map with the execution plan data
        Map<String, Object> executionPlanMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("testOutput", "outputTopic");
        executionPlanMap.put("id", "invalid_rule");
        executionPlanMap.put("input", 2); // INVALID
        executionPlanMap.put("output", outputTopics);
        executionPlanMap.put("executionPlan", "from test select a insert into testOutput");

        // Create the execution plan
        SiddhiPlan.fromMap(executionPlanMap);
    }
}
