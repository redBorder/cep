package net.redborder.correlation.rest;

import net.redborder.correlation.siddhi.RbSiddhiManager;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by jmf on 24/06/15.
 */

@Singleton
@Path("res")
public class Resource {

    RbSiddhiManager rbSiddhiManager = RbSiddhiManager.getRbSiddhiManagerInstance();
    /**
     *
     * Método que manejará las peticiones HTTP POST con formato JSON.
     * Mediante este método se incluirán las querys enviadas por el usuario.
     *
     * @param query Acepta una cadena en formato JSON que contiene la query
     *
     *
     * @return Respuesta con el código obtenido al tratar la petición
     */

    @POST
    @Path("send")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response receiveQuery(String query){

        //Comprobamos que la petición de inclusión ha sido éxitosa
        if(rbSiddhiManager.add(query))
            return Response.status(200).entity(query).build();
        else
            return Response.status(202).entity(query).build();

    }

    /**
     * Método que manejará las peticiones HTTP DELETE.
     * Mediante este método se eliminarán las querys enviadas por el usuario.
     *
     *
     * @param id Se incluirá en la petición el id de la query a eliminar
     *
     * @return Respuesta con el código obtenido al tratar la petición
     */

    @DELETE
    @Path("/delete/{id}")
    @Consumes (MediaType.APPLICATION_JSON)
    public Response deleteQuery(@PathParam("id") String id){


        //Comprobamos que la petición de borrado ha sido éxitosa
        if(rbSiddhiManager.remove(id))
            return Response.status(200).build();
        else
            return Response.status(404).entity("Query with the id " + id + " is not present ").build();


    }
}
