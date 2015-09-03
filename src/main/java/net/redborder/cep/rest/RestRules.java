package net.redborder.cep.rest;

import net.redborder.cep.rest.exceptions.RestException;
import net.redborder.cep.rest.exceptions.RestInvalidException;
import net.redborder.cep.rest.exceptions.RestNotFoundException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the methods available from the HTTP REST API.
 */

@Singleton
@Path("/v1/")
public class RestRules {
    private final Logger log = LoggerFactory.getLogger(RestRules.class);
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * This method handles HTTP POST requests with JSON data.
     * It sends an add operation to the listener passing it the JSON data.
     *
     * @param json A string in JSON format.
     * @return Response with the appropriate HTTP code.
     */

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(String json) {
        RestListener listener = RestManager.getListener();
        Response response;

        try {
            log.info("Add request with json: {}", json);
            listener.add(parseMap(json));
            response = Response.ok().build();
        } catch (RestInvalidException e) {
            log.info("Add request was invalid: {}", e.getMessage());
            response = Response.status(Response.Status.BAD_REQUEST)
                    .entity(toMap(e, Response.Status.BAD_REQUEST))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            response = Response.serverError()
                    .entity(toMap(e, Response.Status.INTERNAL_SERVER_ERROR))
                    .build();
        }

        return response;
    }

    /**
     * This methods handles HTTP DELETE requests.
     * It sends an remove operation to the listener passing it an ID.
     *
     * @param id The ID sent by the user on the request
     * @return Response with the appropriate HTTP code.
     */

    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response remove(@PathParam("id") String id) {
        RestListener listener = RestManager.getListener();
        Response response;

        // Check if the listener accepted the operation
        try {
            log.info("Remove request with id: {}", id);
            listener.remove(id);
            response = Response.ok().build();
        } catch (RestNotFoundException e) {
            log.info("Remove request invalid: {}", e.getMessage());
            e.printStackTrace();
            response = Response.status(Response.Status.NOT_FOUND)
                    .entity(toMap(e, Response.Status.NOT_FOUND))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            response = Response.serverError()
                    .entity(toMap(e, Response.Status.INTERNAL_SERVER_ERROR))
                    .build();
        }

        return response;
    }

    /**
     * This methods handles HTTP POST synchronization requests.
     * It expects a JSON string with a list of maps.
     *
     * @param json The ID sent by the user on the request
     * @return Response with the appropriate HTTP code.
     */

    @POST
    @Path("/synchronize")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response synchronize(String json) {
        RestListener listener = RestManager.getListener();
        Response response;

        try {
            log.info("Synchronize request with json: {}", json);
            listener.synchronize(parseList(json));
            response = Response.ok().build();
        } catch (RestInvalidException e) {
            log.info("Synchronize request was invalid: {}", e.getMessage());
            e.printStackTrace();
            response = Response.status(Response.Status.BAD_REQUEST)
                    .entity(toMap(e, Response.Status.BAD_REQUEST))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            response = Response.serverError()
                    .entity(toMap(e, Response.Status.INTERNAL_SERVER_ERROR))
                    .build();
        }

        return response;
    }

    /**
     * This method handles HTTP GET requests at path /list.
     * It responds with a list in JSON form.
     *
     * @return Response with the appropriate HTTP code.
     */

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response list() {
        RestListener listener = RestManager.getListener();
        Response response;

        try {
            List<Map<String, Object>> list = listener.list();
            log.info("List request {}", list);
            response = Response.ok().entity(list).build();
        } catch (Exception e) {
            e.printStackTrace();
            response = Response.serverError()
                    .entity(toMap(e, Response.Status.INTERNAL_SERVER_ERROR))
                    .build();
        }

        return response;
    }

    // Helper methods

    /**
     * This method returns a map from a given json string
     *
     * @param str The JSON string to parse
     * @return A map with the JSON contents
     * @throws RestException If the string does not have a valid JSON format
     */

    private Map<String, Object> parseMap(String str) throws RestException {
        try {
            Map<String, Object> result = mapper.readValue(str, Map.class);
            return result;
        } catch (IOException e) {
            throw new RestInvalidException("couldn't parse json", e);
        }
    }

    /**
     * This method returns a list from a given json string
     *
     * @param str The JSON string to parse
     * @return A list with the JSON contents
     * @throws RestException If the string does not have a valid JSON format
     */

    private List<Map<String, Object>> parseList(String str) throws RestException {
        try {
            List<Map<String, Object>> result = mapper.readValue(str, List.class);
            return result;
        } catch (IOException e) {
            throw new RestInvalidException("couldn't parse json", e);
        }
    }

    /**
     * Returns a map representation of a throwable object with a given status code.
     * The map has a status entry with the status code, and a message entry with the message
     * of the throwable object.
     *
     * @param e The throwable object
     * @param status The status code
     * @return A map representation of the error
     */

    private Map<String, Object> toMap(Throwable e, Response.Status status) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", status.getStatusCode());
        result.put("message", e.getMessage());
        return result;
    }
}
