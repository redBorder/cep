package net.redborder.correlation.rest;

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

@Singleton
@Path("/")
public class Resource {
    private final Logger log = LoggerFactory.getLogger(Resource.class);
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * This method handles HTTP POST requests with JSON data.
     * It sends an add operation to the listener passing it the JSON data.
     *
     * @param json A string in JSON format.
     *
     * @return Response with the appropriate HTTP code.
     */

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(String json) {
        RestListener listener = RestManager.getListener();
        log.info("Add request with json: {}", json);

        // Check if the listener accepted the data
        try {
            listener.add(parseMap(json));
            return Response.ok().build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(toMap(e)).build();
        }
    }

    /**
     * This methods handles HTTP DELETE requests.
     * It sends an remove operation to the listener passing it an ID.
     *
     * @param id The ID sent by the user on the request
     *
     * @return Response with the appropriate HTTP code.
     */

    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response remove(@PathParam("id") String id) {
        RestListener listener = RestManager.getListener();
        log.info("Remove request with id: {}", id);

        // Check if the listener accepted the operation
        try {
            listener.remove(id);
            return Response.ok().build();
        } catch (NotFoundException e) {
            e.printStackTrace();
            return Response.status(Response.Status.NOT_FOUND).entity(toMap(e)).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(toMap(e)).build();
        }
    }

    /**
     * This methods handles HTTP POST synchronization requests.
     * It expects a JSON string with a list of maps.
     *
     * @param json The ID sent by the user on the request
     *
     * @return Response with the appropriate HTTP code.
     */

    @POST
    @Path("/synchronize")
    @Consumes (MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response synchronize(@PathParam("json") String json) {
        RestListener listener = RestManager.getListener();
        log.info("Synchronize request with json: {}", json);

        // Check if the listener accepted the operation
        try {
            listener.synchronize(parseList(json));
            return Response.ok().build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(toMap(e)).build();
        }
    }

    /**
     * This method handles HTTP GET requests at path /list.
     * It responds with a list in JSON form.
     *
     * @return Response with the appropriate HTTP code.
     */

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response list() {
        RestListener listener = RestManager.getListener();
        log.info("List request");

        try {
            String list = listener.list();
            return Response.ok().entity(list).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(toMap(e)).build();
        }
    }

    /**
     * Helper methods
     */

    private Map<String, Object> parseMap(String str) throws RestException {
        try {
            Map<String, Object> result = mapper.readValue(str, Map.class);
            return result;
        } catch (IOException e) {
            throw new RestException("couldn't parse json", e);
        }
    }

    private List<Map<String, Object>> parseList(String str) throws RestException {
        try {
            List<Map<String, Object>> result = mapper.readValue(str, List.class);
            return result;
        } catch (IOException e) {
            throw new RestException("couldn't parse json", e);
        }
    }

    private Map<String, Object> toMap(Throwable e) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", 500);
        result.put("message", e.getMessage());
        return result;
    }
}
