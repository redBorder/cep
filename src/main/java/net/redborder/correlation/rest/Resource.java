package net.redborder.correlation.rest;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("res")
public class Resource {
    /**
     * This method handles HTTP POST requests with JSON data.
     * It sends an add operation to the listener passing it the JSON data.
     *
     * @param json A string in JSON format.
     *
     * @return Response with the appropriate HTTP code.
     */

    @POST
    @Path("send")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response receiveQuery(String json) {
        RestListener listener = RestManager.getListener();

        // Check if the listener accepted the data
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.add(json)) {
            return Response.status(200).entity(json).build();
        } else {
            return Response.status(202).entity(json).build();
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
    @Path("/delete/{id}")
    @Consumes (MediaType.APPLICATION_JSON)
    public Response deleteQuery(@PathParam("id") String id) {
        RestListener listener = RestManager.getListener();

        // Check if the listener accepted the operation
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.remove(id)) {
            return Response.status(200).build();
        } else {
            return Response.status(404).entity("Query with the id " + id + " is not present").build();
        }
    }
}
