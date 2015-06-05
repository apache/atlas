/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metadata.web.resources;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.repository.graph.GraphProvider;
import org.apache.hadoop.metadata.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Jersey Resource for lineage metadata operations.
 * Implements most of the GET operations of Rexster API with out the indexes.
 * https://github.com/tinkerpop/rexster/wiki/Basic-REST-API
 *
 * This is a subset of Rexster's REST API, designed to provide only read-only methods
 * for accessing the backend graph.
 */
@Path("graph")
@Singleton
public class RexsterGraphResource {
    public static final String OUT_E = "outE";
    public static final String IN_E = "inE";
    public static final String BOTH_E = "bothE";
    public static final String OUT = "out";
    public static final String IN = "in";
    public static final String BOTH = "both";
    public static final String OUT_COUNT = "outCount";
    public static final String IN_COUNT = "inCount";
    public static final String BOTH_COUNT = "bothCount";
    public static final String OUT_IDS = "outIds";
    public static final String IN_IDS = "inIds";
    public static final String BOTH_IDS = "bothIds";
    private static final Logger LOG = LoggerFactory.getLogger(RexsterGraphResource.class);

    private TitanGraph graph;

    @Inject
    public RexsterGraphResource(GraphProvider<TitanGraph> graphProvider) {
        this.graph = graphProvider.get();
    }

    private static void validateInputs(String errorMsg, String... inputs) {
        for (String input : inputs) {
            if (StringUtils.isEmpty(input)) {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                        .entity(errorMsg)
                        .type("text/plain")
                        .build());
            }
        }
    }

    protected Graph getGraph() {
        return graph;
    }

    protected Set<String> getVertexIndexedKeys() {
        return graph.getIndexedKeys(Vertex.class);
    }

    protected Set<String> getEdgeIndexedKeys() {
        return graph.getIndexedKeys(Edge.class);
    }

    /**
     * Get a single vertex with a unique id.
     *
     * GET http://host/metadata/lineage/vertices/id
     * graph.getVertex(id);
     */
    @GET
    @Path("/vertices/{id}")
    @Produces({Servlets.JSON_MEDIA_TYPE})
    public Response getVertex(@PathParam("id") final String vertexId) {
        LOG.info("Get vertex for vertexId= {}", vertexId);
        validateInputs("Invalid argument: vertex id passed is null or empty.", vertexId);
        try {
            Vertex vertex = findVertex(vertexId);

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, GraphSONUtility.jsonFromElement(
                    vertex, getVertexIndexedKeys(), GraphSONMode.NORMAL));
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private Vertex findVertex(String vertexId) {
        Vertex vertex = getGraph().getVertex(vertexId);
        if (vertex == null) {
            String message = "Vertex with [" + vertexId + "] cannot be found.";
            LOG.info(message);
            throw new WebApplicationException(
                    Servlets.getErrorResponse(message, Response.Status.NOT_FOUND));
        }

        return vertex;
    }

    /**
     * Get properties for a single vertex with a unique id.
     * This is NOT a rexster API.
     * <p/>
     * GET http://host/metadata/lineage/vertices/properties/id
     */
    @GET
    @Path("/vertices/properties/{id}")
    @Produces({Servlets.JSON_MEDIA_TYPE})
    public Response getVertexProperties(@PathParam("id") final String vertexId,
                                        @DefaultValue("false") @QueryParam("relationships")
                                        final String relationships) {
        LOG.info("Get vertex for vertexId= {}", vertexId);
        validateInputs("Invalid argument: vertex id passed is null or empty.", vertexId);
        try {
            Vertex vertex = findVertex(vertexId);

            Map<String, String> vertexProperties = getVertexProperties(vertex);

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, new JSONObject(vertexProperties));
            response.put(MetadataServiceClient.COUNT, vertexProperties.size());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private Map<String, String> getVertexProperties(Vertex vertex) {
        Map<String, String> vertexProperties = new HashMap<>();
        for (String key : vertex.getPropertyKeys()) {
            vertexProperties.put(key, vertex.<String>getProperty(key));
        }

        // todo: get the properties from relationships

        return vertexProperties;
    }

    /**
     * Get a list of vertices matching a property key and a value.
     * <p/>
     * GET http://host/metadata/lineage/vertices?key=<key>&value=<value>
     * graph.getVertices(key, value);
     */
    @GET
    @Path("/vertices")
    @Produces({Servlets.JSON_MEDIA_TYPE})
    public Response getVertices(@QueryParam("key") final String key,
                                @QueryParam("value") final String value) {
        LOG.info("Get vertices for property key= {}, value= {}", key, value);
        validateInputs("Invalid argument: key or value passed is null or empty.", key, value);
        try {
            JSONObject response = buildJSONResponse(getGraph().getVertices(key, value));
            return Response.ok(response).build();

        } catch (JSONException e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Get a list of adjacent edges with a direction.
     *
     * GET http://host/metadata/lineage/vertices/id/direction
     * graph.getVertex(id).get{Direction}Edges();
     * direction: {(?!outE)(?!bothE)(?!inE)(?!out)(?!both)(?!in)(?!query).+}
     */
    @GET
    @Path("vertices/{id}/{direction}")
    @Produces({Servlets.JSON_MEDIA_TYPE})
    public Response getVertexEdges(@PathParam("id") String vertexId,
                                   @PathParam("direction") String direction) {
        LOG.info("Get vertex edges for vertexId= {}, direction= {}", vertexId, direction);
        // Validate vertex id. Direction is validated in VertexQueryArguments.
        validateInputs("Invalid argument: vertex id or direction passed is null or empty.",
                vertexId, direction);
        try {
            Vertex vertex = findVertex(vertexId);

            return getVertexEdges(vertex, direction);

        } catch (JSONException e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private Response getVertexEdges(Vertex vertex, String direction) throws JSONException {
        // break out the segment into the return and the direction
        VertexQueryArguments queryArguments = new VertexQueryArguments(direction);
        // if this is a query and the _return is "count" then we don't bother to send back the
        // result array
        boolean countOnly = queryArguments.isCountOnly();
        // what kind of data the calling client wants back (vertices, edges, count, vertex
        // identifiers)
        ReturnType returnType = queryArguments.getReturnType();
        // the query direction (both, out, in)
        Direction queryDirection = queryArguments.getQueryDirection();

        VertexQuery query = vertex.query().direction(queryDirection);

        JSONArray elementArray = new JSONArray();
        long counter = 0;
        if (returnType == ReturnType.VERTICES || returnType == ReturnType.VERTEX_IDS) {
            Iterable<Vertex> vertexQueryResults = query.vertices();
            for (Vertex v : vertexQueryResults) {
                if (returnType.equals(ReturnType.VERTICES)) {
                    elementArray.put(GraphSONUtility.jsonFromElement(
                            v, getVertexIndexedKeys(), GraphSONMode.NORMAL));
                } else {
                    elementArray.put(v.getId());
                }
                counter++;
            }
        } else if (returnType == ReturnType.EDGES) {
            Iterable<Edge> edgeQueryResults = query.edges();
            for (Edge e : edgeQueryResults) {
                elementArray.put(GraphSONUtility.jsonFromElement(
                        e, getEdgeIndexedKeys(), GraphSONMode.NORMAL));
                counter++;
            }
        } else if (returnType == ReturnType.COUNT) {
            counter = query.count();
        }

        JSONObject response = new JSONObject();
        if (!countOnly) {
            response.put(MetadataServiceClient.RESULTS, elementArray);
        }
        response.put(MetadataServiceClient.COUNT, counter);
        return Response.ok(response).build();
    }

    /**
     * Get a single edge with a unique id.
     *
     * GET http://host/metadata/lineage/edges/id
     * graph.getEdge(id);
     */
    @GET
    @Path("/edges/{id}")
    @Produces({Servlets.JSON_MEDIA_TYPE})
    public Response getEdge(@PathParam("id") final String edgeId) {
        LOG.info("Get vertex for edgeId= {}", edgeId);
        validateInputs("Invalid argument: edge id passed is null or empty.", edgeId);
        try {
            Edge edge = getGraph().getEdge(edgeId);
            if (edge == null) {
                String message = "Edge with [" + edgeId + "] cannot be found.";
                LOG.info(message);
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
                        .entity(Servlets.escapeJsonString(message)).build());
            }

            JSONObject response = new JSONObject();
            response.put(MetadataServiceClient.RESULTS, GraphSONUtility.jsonFromElement(
                    edge, getEdgeIndexedKeys(), GraphSONMode.NORMAL));
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(
                    Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private <T extends Element> JSONObject buildJSONResponse(Iterable<T> elements)
    throws JSONException {
        JSONArray vertexArray = new JSONArray();
        long counter = 0;
        for (Element element : elements) {
            counter++;
            vertexArray.put(GraphSONUtility.jsonFromElement(
                    element, getVertexIndexedKeys(), GraphSONMode.NORMAL));
        }

        JSONObject response = new JSONObject();
        response.put(MetadataServiceClient.RESULTS, vertexArray);
        response.put(MetadataServiceClient.COUNT, counter);

        return response;
    }
    private enum ReturnType {VERTICES, EDGES, COUNT, VERTEX_IDS}

    /**
     * Helper class for query arguments.
     */
    public static final class VertexQueryArguments {

        private final Direction queryDirection;
        private final ReturnType returnType;
        private final boolean countOnly;

        public VertexQueryArguments(String directionSegment) {
            if (OUT_E.equals(directionSegment)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else if (IN_E.equals(directionSegment)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (BOTH_E.equals(directionSegment)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (OUT.equals(directionSegment)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else if (IN.equals(directionSegment)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (BOTH.equals(directionSegment)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (BOTH_COUNT.equals(directionSegment)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.BOTH;
                countOnly = true;
            } else if (IN_COUNT.equals(directionSegment)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.IN;
                countOnly = true;
            } else if (OUT_COUNT.equals(directionSegment)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.OUT;
                countOnly = true;
            } else if (BOTH_IDS.equals(directionSegment)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (IN_IDS.equals(directionSegment)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (OUT_IDS.equals(directionSegment)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                        .entity(Servlets.escapeJsonString(directionSegment + " segment was invalid."))
                        .build());
            }
        }

        public Direction getQueryDirection() {
            return queryDirection;
        }

        public ReturnType getReturnType() {
            return returnType;
        }

        public boolean isCountOnly() {
            return countOnly;
        }
    }
}
