package net.redborder.correlation.rest;


import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by jmf on 11/06/15.
 */
public class RESTManager {

    private final Logger log = LoggerFactory.getLogger(RESTManager.class);

    // Base URI the Grizzly HTTP server will listen on
    private String BASE_URI = "http://localhost:8888/myapp/";
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

        if (rc != null)
            this.startServer();
        else
            log.error("Configuration is not valid");

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

        if(server != null)
            log.info("Starting server...");
        else
            log.error("Server is not initialized");
    }

    public void stopServer(){
        if(server != null)
            server.shutdown();
    }

}
