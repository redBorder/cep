package net.redborder.correlation.rest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class RestManager {
    private final Logger log = LoggerFactory.getLogger(RestManager.class);

    // Base URI the Grizzly HTTP server will listen on
    private String BASE_URI = "http://localhost:8888/myapp/";
    private ResourceConfig rc;
    private HttpServer server;

    /**
     * TODO: Get URI from ConfigData
     */
    public RestManager() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        rc = new ResourceConfig().packages("net.redborder.correlation.rest");
        this.startServer();
    }

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     */
    public void startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        // rc = new ResourceConfig().packages("net.redborder.correlation.rest");

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
        log.info("Starting server...");
    }

    public void stopServer() {
        if(server != null)
            server.shutdown();
    }
}
