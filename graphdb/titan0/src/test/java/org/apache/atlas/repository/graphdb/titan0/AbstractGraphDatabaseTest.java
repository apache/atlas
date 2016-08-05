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

package org.apache.atlas.repository.graphdb.titan0;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

/**
 *
 */
public abstract class AbstractGraphDatabaseTest {

    private static class RunnableWrapper implements Runnable {
        private final Runnable r;
        private Throwable exceptionThrown_ = null;

        private RunnableWrapper(Runnable r) {
            this.r = r;
        }

        @Override
        public void run() {
            try {
                r.run();
            }
            catch(Throwable e) {
                exceptionThrown_ = e;
            }

        }

        public Throwable getExceptionThrown() {
            return exceptionThrown_;
        }
    }

    protected static final String WEIGHT_PROPERTY = "weight";
    protected static final String TRAIT_NAMES = Constants.TRAIT_NAMES_PROPERTY_KEY;
    protected static final String typeProperty = "__type";
    protected static final String typeSystem = "typeSystem";

    /**
     *
     */
    private static final String BACKING_INDEX_NAME = "backing";

    private AtlasGraph<?,?> graph = null;

    @AfterMethod
    public void commitGraph() {
        //force any pending actions to be committed so we can be sure they don't cause errors.
        pushChangesAndFlushCache();
        graph.commit();
    }
    protected <V, E> void pushChangesAndFlushCache() {
        AtlasGraph<V, E> graph = getGraph();
        graph.commit();
    }


    @BeforeClass
    public static void createIndices() {
        Titan0Database db = new Titan0Database();
        AtlasGraphManagement mgmt = db.getGraph().getManagementSystem();

        if(mgmt.getGraphIndex(BACKING_INDEX_NAME) == null) {
            mgmt.buildMixedVertexIndex(BACKING_INDEX_NAME, Constants.BACKING_INDEX);
        }
        mgmt.makePropertyKey("age13",Integer.class, Multiplicity.OPTIONAL);

        createIndices(mgmt, "name", String.class, false, Multiplicity.REQUIRED);
        createIndices(mgmt, WEIGHT_PROPERTY, Integer.class, false, Multiplicity.OPTIONAL);
        createIndices(mgmt, "size15", String.class, false, Multiplicity.REQUIRED);
        createIndices(mgmt, "typeName", String.class, false, Multiplicity.REQUIRED);
        createIndices(mgmt, "__type", String.class, false, Multiplicity.REQUIRED);
        createIndices(mgmt, Constants.GUID_PROPERTY_KEY, String.class, true, Multiplicity.REQUIRED);
        createIndices(mgmt, Constants.TRAIT_NAMES_PROPERTY_KEY, String.class, false, Multiplicity.SET);
        createIndices(mgmt, Constants.SUPER_TYPES_PROPERTY_KEY, String.class, false, Multiplicity.SET);
        mgmt.commit();
    }

    @AfterClass
    public static void cleanUp() {
        Titan0Graph graph = new Titan0Graph();
        graph.clear();

    }

    private static void createIndices(AtlasGraphManagement management, String propertyName, Class propertyClass,
            boolean isUnique, Multiplicity cardinality) {



        if(management.containsPropertyKey(propertyName)) {
            //index was already created
            return;
        }

        AtlasPropertyKey key = management.makePropertyKey(propertyName, propertyClass, cardinality);
        try {
            if(propertyClass != Integer.class) {
                management.addIndexKey(BACKING_INDEX_NAME, key);
            }
        }
        catch(Throwable t) {
            //ok
            t.printStackTrace();
        }
        try {
            //if(propertyClass != Integer.class) {
            management.createCompositeIndex(propertyName, key, isUnique);
            //}
        }
        catch(Throwable t) {
            //ok
            t.printStackTrace();
        }


    }

    /**
     *
     */
    public AbstractGraphDatabaseTest() {
        super();
    }


    protected final <V,E> AtlasGraph<V, E> getGraph() {
        if(graph == null) {
            graph = new Titan0Graph();
        }
        return (AtlasGraph<V,E>)graph;
    }

    protected Titan0Graph getTitan0Graph() {
        AtlasGraph g = getGraph();
        return (Titan0Graph)g;
    }


    protected List<AtlasVertex> newVertices_ = new ArrayList<>();

    protected final <V, E> AtlasVertex<V, E> createVertex(AtlasGraph<V, E> graph) {
        AtlasVertex<V,E> vertex = graph.addVertex();
        newVertices_.add(vertex);
        return vertex;
    }

    @AfterMethod
    public void removeVertices() {
        for(AtlasVertex vertex : newVertices_) {
            if(vertex.exists()) {
                getGraph().removeVertex(vertex);
            }
        }
        getGraph().commit();
        newVertices_.clear();
    }
    protected void runSynchronouslyInNewThread(final Runnable r) throws Throwable {

        RunnableWrapper wrapper = new RunnableWrapper(r);
        Thread th = new Thread(wrapper);
        th.start();
        th.join();
        Throwable ex = wrapper.getExceptionThrown();
        if(ex != null) {
            throw ex;
        }
    }


}