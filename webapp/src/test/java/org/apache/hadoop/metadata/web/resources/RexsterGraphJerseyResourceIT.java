package org.apache.hadoop.metadata.web.resources;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Integration tests for Rexster Graph Jersey Resource.
 */
public class RexsterGraphJerseyResourceIT extends BaseResourceIT {

    @Test (enabled = false)
    public void testGetVertex() throws Exception {
        // todo: add a vertex before fetching it

        WebResource resource = service
                .path("api/metadata/graph/vertices")
                .path("0");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        String response = clientResponse.getEntity(String.class);
        Assert.assertNotNull(response);
    }

    @Test
    public void testGetVertexWithInvalidId() throws Exception {
        WebResource resource = service
                .path("api/metadata/graph/vertices/blah");

        ClientResponse clientResponse = resource
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testGetVertexProperties() throws Exception {

    }

    @Test
    public void testGetVertices() throws Exception {

    }

    @Test
    public void testGetVertexEdges() throws Exception {

    }

    @Test
    public void testGetEdge() throws Exception {

    }
}
