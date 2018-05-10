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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.testng.AssertJUnit.assertTrue;

public class BaseUtils {
    private static final String resourcesDirRelativePath = "/src/test/resources/";
    private String resourceDir;

    protected final RelationshipTypeCache emptyRelationshipCache = new RelationshipTypeCache(new HashMap<>());
    protected GraphSONUtility graphSONUtility;

    protected Object[][] getJsonNodeFromFile(String s) throws IOException {
        File f = new File(getFilePath(s));
        return new Object[][]{{getEntityNode(FileUtils.readFileToString(f))}};
    }

    protected String getFilePath(String fileName) {
        return Paths.get(resourceDir, fileName).toString();
    }

    @BeforeClass
    public void setup() {
        resourceDir = System.getProperty("user.dir") + resourcesDirRelativePath;
        graphSONUtility = new GraphSONUtility(emptyRelationshipCache);
    }

    protected Object getId(JsonNode node) {
        GraphSONUtility gu = graphSONUtility;
        return gu.getTypedValueFromJsonNode(node.get(GraphSONTokensTP2._ID));
    }


    private JsonNode getEntityNode(String json) throws IOException {
        GraphSONMapper.Builder builder = GraphSONMapper.build();
        final ObjectMapper mapper  = builder.typeInfo(TypeInfo.NO_TYPES).create().createMapper();
        return mapper.readTree(json);
    }

    protected void addVertex(TinkerGraph tg, JsonNode node) {
        GraphSONUtility utility = new GraphSONUtility(emptyRelationshipCache);
        utility.vertexFromJson(tg, node);
    }

    protected void addEdge(TinkerGraph tg, MappedElementCache cache) throws IOException {
        GraphSONUtility gu = graphSONUtility;

        gu.vertexFromJson(tg, (JsonNode) (getDBV(null)[0][0]));
        gu.vertexFromJson(tg, (JsonNode) (getTableV(null))[0][0]);
        gu.edgeFromJson(tg, cache, (JsonNode) getEdge(null)[0][0]);
    }

    protected Vertex fetchTableVertex(TinkerGraph tg) {
        GraphTraversal query = tg.traversal().V().has("__typeName", "hive_table");
        assertTrue(query.hasNext());

        return (Vertex) query.next();
    }

    @DataProvider(name = "col1")
    public Object[][] getCol1(ITestContext context) throws IOException {
        return getJsonNodeFromFile("col-legacy.json");
    }

    @DataProvider(name = "dbType")
    public Object[][] getDbType(ITestContext context) throws IOException {
        return getJsonNodeFromFile("db-type-legacy.json");
    }

    @DataProvider(name = "edge")
    public Object[][] getEdge(ITestContext context) throws IOException {
        return getJsonNodeFromFile("edge-legacy.json");
    }

    @DataProvider(name = "dbV")
    public Object[][] getDBV(ITestContext context) throws IOException {
        return getJsonNodeFromFile("db-v-65544.json");
    }


    @DataProvider(name = "tableV")
    public Object[][] getTableV(ITestContext context) throws IOException {
        return getJsonNodeFromFile("table-v-147504.json");
    }
}
