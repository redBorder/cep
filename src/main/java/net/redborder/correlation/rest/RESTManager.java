package net.redborder.correlation.rest;


import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

/**
 * Created by jmf on 11/06/15.
 */
public class RESTManager {

    // Base URI the Grizzly HTTP server will listen on
    private String BASE_URI = "http://localhost:8080/myapp/";
    private ResourceConfig rc;
    private HttpServer server;


    /*Realizar constructor que establezca la configuracion:
    *       -URI:ip/port/
    *
    * */
    public RESTManager(){
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        rc = new ResourceConfig().packages("net.redborder.correlation.rest");

    }

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public void startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        //rc = new ResourceConfig().packages("net.redborder.correlation.rest");

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    public void stopServer(){

        server.stop();
    }

}
