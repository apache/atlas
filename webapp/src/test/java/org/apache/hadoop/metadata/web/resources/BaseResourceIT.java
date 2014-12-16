package org.apache.hadoop.metadata.web.resources;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.core.UriBuilder;

public class BaseResourceIT {

    protected WebResource service;

    @BeforeClass
    public void setUp() throws Exception {
        String baseUrl = "http://localhost:21000/";

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }
}
