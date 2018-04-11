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

package org.apache.atlas.migration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementPropertyConfig;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public class AtlasGraphSONWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasGraphSONWriter.class);

    private static final JsonFactory jsonFactory = new MappingJsonFactory();
    private final Graph graph;

    /**
     * @param graph the Graph to pull the data from
     */
    private AtlasGraphSONWriter(final Graph graph) {
        this.graph = graph;
    }

    public void outputGraph(final OutputStream jsonOutputStream, final Set<String> vertexPropertyKeys,
                            final Set<String> edgePropertyKeys, final GraphSONMode mode, final boolean normalize) throws IOException {
        final JsonGenerator jg = jsonFactory.createGenerator(jsonOutputStream);

        // don't let the JsonGenerator close the underlying stream...leave that to the client passing in the stream
        jg.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        final AtlasGraphSONUtility graphson = new AtlasGraphSONUtility(mode,
                ElementPropertyConfig.includeProperties(vertexPropertyKeys, edgePropertyKeys, normalize));

        jg.writeStartObject();  // start
        jg.writeStringField(GraphSONTokens.MODE, mode.toString());

        LOG.info("Starting vertices...");
        jg.writeArrayFieldStart(GraphSONTokens.VERTICES);

        final Iterable<Vertex> vertices = vertices();
        long vertexCount = 0;
        for (Vertex v : vertices) {
            jg.writeTree(graphson.objectNodeFromElement(v));
            vertexCount++;
        }

        jg.writeEndArray(); // vertices end

        LOG.info("Starting edges...");
        jg.writeArrayFieldStart(GraphSONTokens.EDGES);

        final Iterable<Edge> edges = edges();
        long edgeCount = 0;
        for (Edge e : edges) {
            jg.writeTree(graphson.objectNodeFromElement(e));
            edgeCount++;
        }

        jg.writeEndArray(); // edges end

        writeMetrics(jg, vertexCount, edgeCount);

        jg.writeEndObject(); // end
        jg.flush();
        jg.close();
    }

    private void writeMetrics(JsonGenerator jg, long vertexCount, long edgeCount) throws IOException {
        jg.writeNumberField("vertexCount", vertexCount);
        jg.writeNumberField("edgeCount", edgeCount);
    }

    private Iterable<Vertex> vertices() {
        return graph.getVertices();
    }

    private Iterable<Edge> edges() {
        return graph.getEdges();
    }


    /**
     * Write the data in a Graph to a JSON OutputStream. All keys are written to JSON.
     *
     * @param graph    the graph to serialize to JSON
     * @param fos      the JSON file to write the Graph data to
     * @param mode     determines the format of the GraphSON
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(Graph graph, FileOutputStream fos, GraphSONMode mode) throws IOException {
        final AtlasGraphSONWriter writer = new AtlasGraphSONWriter(graph);
        writer.outputGraph(fos, null, null, mode, false);
    }

}
