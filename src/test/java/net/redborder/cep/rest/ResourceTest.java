package net.redborder.cep.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.redborder.cep.siddhi.SiddhiHandler;
import net.redborder.cep.util.ConfigData;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URL;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ResourceTest extends JerseyTest {
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected Application configure() {
        return new ResourceConfig(Resource.class);
    }

    @BeforeClass
    public static void init() {
        URL testConfigPath = ResourceTest.class.getClassLoader().getResource("config.yml");
        ConfigData.setConfigFile(testConfigPath.getFile());
        SiddhiHandler siddhiHandler = new SiddhiHandler();
        RestManager.startServer("http://localhost:8080/", siddhiHandler);
    }

    @Test
    public void add() throws Exception {
        // Create a rule that will be added
        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "testID");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");

        String json = objectMapper.writeValueAsString(elementMap);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.OK.getStatusCode(), statusCode);
    }

    @Test
    public void addInvalid() throws Exception {
        // Create an invalid rule that will be added
        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "testID");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        // elementMap.put("executionPlan", "from test select src, bytes insert into outputStream;");

        String json = objectMapper.writeValueAsString(elementMap);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void addInvalidType() throws Exception {
        // Create an invalid rule that will be added
        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "testID");
        elementMap.put("input", 2); // INVALID TYPE
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select src, bytes insert into outputStream;");

        String json = objectMapper.writeValueAsString(elementMap);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void addNoJSON() throws Exception {
        String json = "NO_JSON_STRING";

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void remove() throws Exception {
        // Create a rule that will be added
        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "rule_to_delete");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");

        // Serialize the map as a json string
        String json = objectMapper.writeValueAsString(elementMap);

        // Call the REST API to add the rule
        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

        // Now remove it
        response = target("/v1/rule_to_delete").request(MediaType.APPLICATION_JSON_TYPE).delete();
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }

    @Test
    public void removeNotPresent() throws Exception {
        // Remove a rule not present
        Response response = target("/rule_not_present").request(MediaType.APPLICATION_JSON_TYPE).delete();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }

    @Test
    public void synchronize() throws Exception {
        // Create a set of rules that will be added
        List<Map<String, Object>> listOfRules = new ArrayList<>();

        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "rule_list_one");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap);

        Map<String, Object> elementMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("outputStream", "outputTopic");
        elementMap2.put("id", "rule_list_two");
        elementMap2.put("input", Arrays.asList("test"));
        elementMap2.put("output", outputTopics2);
        elementMap2.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap2);

        String json = objectMapper.writeValueAsString(listOfRules);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/synchronize").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.OK.getStatusCode(), statusCode);
    }

    @Test
    public void synchronizeInvalid() throws Exception {
        // Create a set of rules that will be added
        List<Map<String, Object>> listOfRules = new ArrayList<>();

        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "rule_list_one");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap);

        Map<String, Object> elementMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("outputStream", "outputTopic");
        // elementMap2.put("id", "rule_list_two");
        elementMap2.put("input", Arrays.asList("test"));
        elementMap2.put("output", outputTopics2);
        elementMap2.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap2);

        String json = objectMapper.writeValueAsString(listOfRules);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/synchronize").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void synchronizeInvalidType() throws Exception {
        // Create a set of rules that will be added
        List<Map<String, Object>> listOfRules = new ArrayList<>();

        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "rule_list_one");
        elementMap.put("input", 2); // INVALID TYPE
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap);

        Map<String, Object> elementMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("outputStream", "outputTopic");
        // elementMap2.put("id", "rule_list_two");
        elementMap2.put("input", Arrays.asList("test"));
        elementMap2.put("output", outputTopics2);
        elementMap2.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap2);

        String json = objectMapper.writeValueAsString(listOfRules);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/synchronize").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void synchronizeWithSameID() throws Exception {
        // Create a set of rules that will be added
        List<Map<String, Object>> listOfRules = new ArrayList<>();

        Map<String, Object> elementMap = new HashMap<>();
        Map<String, String> outputTopics = new HashMap<>();
        outputTopics.put("outputStream", "outputTopic");
        elementMap.put("id", "rule_list_one");
        elementMap.put("input", Arrays.asList("test"));
        elementMap.put("output", outputTopics);
        elementMap.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap);

        Map<String, Object> elementMap2 = new HashMap<>();
        Map<String, String> outputTopics2 = new HashMap<>();
        outputTopics2.put("outputStream", "outputTopic");
        elementMap2.put("id", "rule_list_one");
        elementMap2.put("input", Arrays.asList("test"));
        elementMap2.put("output", outputTopics2);
        elementMap2.put("executionPlan", "from test select a, b insert into outputStream;");
        listOfRules.add(elementMap2);

        String json = objectMapper.writeValueAsString(listOfRules);

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/synchronize").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void synchronizeBadJSON() throws Exception {
        String json = "NOT_A_JSON_STRING";

        Entity<String> entity = Entity.json(json);
        Response response = target("/v1/synchronize").request(MediaType.APPLICATION_JSON_TYPE).post(entity);
        int statusCode = response.getStatus();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), statusCode);
    }

    @Test
    public void list() throws Exception {
        Response response = target("/v1/").request(MediaType.APPLICATION_JSON_TYPE).get();
        int statusCode = response.getStatus();

        assertEquals(Response.Status.OK.getStatusCode(), statusCode);
    }
}
