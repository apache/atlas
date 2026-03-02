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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.apache.atlas.*;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.VertexEdgePropertiesCache;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.graphdb.janus.*;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.util.BeanUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.exception.EntityNotFoundException;
import org.apache.atlas.util.AttributeValueMap;
import org.apache.atlas.util.IndexedInstance;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.*;

import static org.apache.atlas.AtlasConfiguration.MAX_EDGES_SUPER_VERTEX;
import static org.apache.atlas.AtlasConfiguration.TIMEOUT_SUPER_VERTEX_FETCH;
import static org.apache.atlas.AtlasErrorCode.RELATIONSHIP_CREATE_INVALID_PARAMS;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.BOTH;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.IN;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;
import static org.apache.atlas.type.Constants.HAS_LINEAGE_VALID;

/**
 * Utility class for graph operations.
 */
public final class GraphHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphHelper.class);
    public static final String EDGE_LABEL_PREFIX = "__";

    public static final String RETRY_COUNT = "atlas.graph.storage.num.retries";
    public static final String RETRY_DELAY = "atlas.graph.storage.retry.sleeptime.ms";
    public static final String DEFAULT_REMOVE_PROPAGATIONS_ON_ENTITY_DELETE = "atlas.graph.remove.propagations.default";

    private AtlasGraph graph;

    private int     maxRetries = 3;
    private long    retrySleepTimeMillis = 1000;
    private boolean removePropagations = true;

    public GraphHelper(AtlasGraph graph) {
        this.graph = graph;
        try {
            maxRetries           = ApplicationProperties.get().getInt(RETRY_COUNT, 3);
            retrySleepTimeMillis = ApplicationProperties.get().getLong(RETRY_DELAY, 1000);
            removePropagations   = ApplicationProperties.get().getBoolean(DEFAULT_REMOVE_PROPAGATIONS_ON_ENTITY_DELETE, true);
        } catch (AtlasException e) {
            LOG.error("Could not load configuration. Setting to default value for " + RETRY_COUNT, e);
        }
    }

    public static boolean isTermEntityEdge(AtlasEdge edge) {
        return StringUtils.equals(edge.getLabel(), TERM_ASSIGNMENT_LABEL);
    }

    public AtlasEdge addClassificationEdge(AtlasVertex entityVertex, AtlasVertex classificationVertex, boolean isPropagated) throws AtlasBaseException {
        AtlasEdge ret = addEdge(entityVertex, classificationVertex, CLASSIFICATION_LABEL);

        if (ret != null) {
            AtlasGraphUtilsV2.setEncodedProperty(ret, CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex));
            AtlasGraphUtilsV2.setEncodedProperty(ret, CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, isPropagated);
        }

        return ret;
    }

    public AtlasEdge addEdge(AtlasVertex fromVertex, AtlasVertex toVertex, String edgeLabel) throws AtlasBaseException {
        AtlasEdge ret;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding edge for {} -> label {} -> {}", string(fromVertex), edgeLabel, string(toVertex));
        }

        String fromGuid = getGuid(fromVertex);
        if (fromGuid != null && fromGuid.equals(getGuid(toVertex))) {
            LOG.error("Attempting to create a relationship between same vertex with guid {}", fromGuid);
            throw new AtlasBaseException(RELATIONSHIP_CREATE_INVALID_PARAMS, fromGuid);
        }

        ret = graph.addEdge(fromVertex, toVertex, edgeLabel);

        if (ret != null) {
            AtlasGraphUtilsV2.setEncodedProperty(ret, STATE_PROPERTY_KEY, ACTIVE.name());
            AtlasGraphUtilsV2.setEncodedProperty(ret, TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            AtlasGraphUtilsV2.setEncodedProperty(ret, CREATED_BY_KEY, RequestContext.get().getUser());
            AtlasGraphUtilsV2.setEncodedProperty(ret, MODIFIED_BY_KEY, RequestContext.get().getUser());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Added {}", string(ret));
            }
        }

        return ret;
    }

    public AtlasEdge getEdge(AtlasVertex outVertex, AtlasVertex inVertex, String edgeLabel) throws RepositoryException, AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getEdge");

        if (inVertex.hasEdges(AtlasEdgeDirection.IN, edgeLabel) && outVertex.hasEdges(AtlasEdgeDirection.OUT, edgeLabel)) {
            AtlasEdge edge = graph.getEdgeBetweenVertices(outVertex, inVertex, edgeLabel);
                return edge;
        }

        RequestContext.get().endMetricRecord(metric);
        return null;
    }

    public AtlasEdge getOrCreateEdge(AtlasVertex outVertex, AtlasVertex inVertex, String edgeLabel) throws RepositoryException, AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getOrCreateEdge");
        boolean skipRetry = false;

        for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Running edge creation attempt {}", numRetries);
                }

                AtlasEdge edge = graph.getEdgeBetweenVertices(outVertex, inVertex, edgeLabel);
                if (edge != null) {
                    return edge;
                }

                try {
                    return addEdge(outVertex, inVertex, edgeLabel);
                } catch (AtlasBaseException abe) {
                    if (abe.getAtlasErrorCode().getErrorCode().equals(RELATIONSHIP_CREATE_INVALID_PARAMS.getErrorCode())) {
                        skipRetry = true;
                        throw abe;
                    }
                }
            } catch (Exception e) {
                if (skipRetry) {
                    throw e;
                }
                LOG.warn(String.format("Exception while trying to create edge from %s to %s with label %s. Retrying",
                        vertexString(outVertex), vertexString(inVertex), edgeLabel), e);

                if (numRetries == (maxRetries - 1)) {
                    LOG.error("Max retries exceeded for edge creation {} {} {} ", outVertex, inVertex, edgeLabel, e);
                    throw new RepositoryException("Edge creation failed after retries", e);
                }

                try {
                    LOG.info("Retrying with delay of {} ms ", retrySleepTimeMillis);
                    Thread.sleep(retrySleepTimeMillis);
                } catch(InterruptedException ie) {
                    LOG.warn("Retry interrupted during edge creation ");
                    throw new RepositoryException("Retry interrupted during edge creation", ie);
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);
        return null;
    }

    public AtlasEdge getEdgeByEdgeId(AtlasVertex outVertex, String edgeLabel, String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return graph.getEdge(edgeId);

        //TODO get edge id is expensive. Use this logic. But doesn't work for now
        /**
        Iterable<AtlasEdge> edges = outVertex.getEdges(Direction.OUT, edgeLabel);
        for (AtlasEdge edge : edges) {
            if (edge.getObjectId().toString().equals(edgeId)) {
                return edge;
            }
        }
        return null;
         **/
    }

    /**
     * Args of the format prop1, key1, prop2, key2...
     * Searches for a AtlasVertex with prop1=key1 && prop2=key2
     * @param args
     * @return AtlasVertex with the given property keys
     * @throws AtlasBaseException
     */
    public AtlasVertex findVertex(Object... args) throws EntityNotFoundException {
        return (AtlasVertex) findElement(true, args);
    }

    /**
     * Args of the format prop1, key1, prop2, key2...
     * Searches for a AtlasEdge with prop1=key1 && prop2=key2
     * @param args
     * @return AtlasEdge with the given property keys
     * @throws AtlasBaseException
     */
    public AtlasEdge findEdge(Object... args) throws EntityNotFoundException {
        return (AtlasEdge) findElement(false, args);
    }

    private AtlasElement findElement(boolean isVertexSearch, Object... args) throws EntityNotFoundException {
        AtlasGraphQuery query = graph.query();

        for (int i = 0; i < args.length; i += 2) {
            query = query.has((String) args[i], args[i + 1]);
        }

        Iterator<AtlasElement> results = null;
        try {
            results = isVertexSearch ? query.vertices().iterator() : query.edges().iterator();
            AtlasElement element = (results != null && results.hasNext()) ? results.next() : null;

            if (element == null) {
                throw new EntityNotFoundException("Could not find " + (isVertexSearch ? "vertex" : "edge") + " with condition: " + getConditionString(args));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Found {} with condition {}", string(element), getConditionString(args));
            }

            return element;
        } finally {
            // Close iterator to release resources
            if (results instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) results).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }
    }

    //In some cases of parallel APIs, the edge is added, but get edge by label doesn't return the edge. ATLAS-1104
    //So traversing all the edges
    public static Iterator<AtlasEdge> getAdjacentEdgesByLabel(AtlasVertex instanceVertex, AtlasEdgeDirection direction, final String edgeLabel) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getAdjacentEdgesByLabel");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finding edges for {} with label {}", string(instanceVertex), edgeLabel);
        }

        Iterator<AtlasEdge> ret = null;
        if (instanceVertex != null && edgeLabel != null) {
            AtlasJanusVertex janusVertex = (AtlasJanusVertex) instanceVertex;
            AtlasJanusGraph  janusGraph  = janusVertex.getAtlasJanusGraph();
            var              g           = janusGraph.getGraph().traversal();
            Stream<Edge>     edgeStream;

            if (direction == AtlasEdgeDirection.IN) {
                edgeStream = g.V(instanceVertex.getId()).inE(edgeLabel).toStream();
            } else if (direction == AtlasEdgeDirection.OUT) {
                edgeStream = g.V(instanceVertex.getId()).outE(edgeLabel).toStream();
            } else {
                edgeStream = g.V(instanceVertex.getId()).bothE(edgeLabel).toStream();
            }

            ret = (Iterator<AtlasEdge>) (Iterator) edgeStream
                    .map(e -> GraphDbObjectFactory.createEdge(janusGraph, e))
                    .iterator();
        }

        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    public static long getAdjacentEdgesCountByLabel(AtlasVertex instanceVertex, AtlasEdgeDirection direction, final String edgeLabel) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getAdjacentEdgesCountByLabel");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finding edges for {} with label {}", string(instanceVertex), edgeLabel);
        }

        long ret = 0;
        if(instanceVertex != null && edgeLabel != null) {
            ret = instanceVertex.getEdgesCount(direction, edgeLabel);
        }

        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    public static boolean isPropagationEnabled(AtlasVertex classificationVertex) {
        boolean ret = false;

        if (classificationVertex != null) {
            Boolean enabled = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VERTEX_PROPAGATE_KEY, Boolean.class);

            ret = (enabled == null) ? true : enabled;
        }

        return ret;
    }

    public static boolean getRemovePropagations(AtlasVertex classificationVertex) {
        boolean ret = false;
        if (classificationVertex != null) {
            Boolean enabled = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY, Boolean.class);
            ret = enabled == null || enabled;
        }
        return ret;
    }

    public static boolean getRemovePropagations(Map<String, Object> classificationPropertiesMap) {
        boolean ret;
        Boolean enabled = (Boolean) classificationPropertiesMap.get(CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY);
        ret = enabled == null || enabled;
        return ret;
    }

    public static boolean getRestrictPropagation(AtlasVertex classificationVertex, String propertyName) {
        if (classificationVertex == null) {
            return false;
        }
        Boolean restrictPropagation = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, propertyName, Boolean.class);
        return restrictPropagation != null && restrictPropagation;
    }

    public static boolean getRestrictPropagation(Map<String, Object> classificationPropertiesMap, String propertyName) {
        Boolean restrictPropagation = (Boolean) classificationPropertiesMap.get(propertyName);

        return restrictPropagation != null && restrictPropagation;
    }

    public static boolean getRestrictPropagationThroughLineage(AtlasVertex classificationVertex) {
        return getRestrictPropagation(classificationVertex,CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE);
    }

    public static boolean getRestrictPropagationThroughHierarchy(AtlasVertex classificationVertex) {
        return getRestrictPropagation(classificationVertex,CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY);
    }

    public static boolean getRestrictPropagationThroughLineage(Map<String, Object> classificationPropertiesMap) {
        return getRestrictPropagation(classificationPropertiesMap, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE);
    }

    public static boolean getRestrictPropagationThroughHierarchy(Map<String, Object> classificationPropertiesMap) {
        return getRestrictPropagation(classificationPropertiesMap, CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY);
    }

    public void repairTagVertex(AtlasEdge edge, AtlasVertex classificationVertex) {
        LOG.info("Repairing corrupt tag-vertex");
        removeEdge(edge);
        removeVertex(classificationVertex);
    }

    public static AtlasVertex getClassificationVertex(GraphHelper graphHelper, AtlasVertex entityVertex, String classificationName) {
        AtlasVertex ret   = null;
        Iterable    edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL)
                                                .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, false)
                                                .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, classificationName).edges();

        if (edges != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                while (iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();
                    if(Objects.nonNull(edge))
                    {
                        AtlasVertex classificationVertex = edge.getInVertex();
                        if(Objects.nonNull(classificationVertex) && StringUtils.isNotEmpty(classificationVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class))) {
                            return edge.getInVertex();
                        } else if(graphHelper != null) {
                            graphHelper.repairTagVertex(edge, edge.getInVertex());
                        }
                    }
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }
    public static Iterator<AtlasVertex> getClassificationVertices(AtlasGraph graph, String classificationName, int size) {
        Iterable classificationVertices = graph.query().has(TYPE_NAME_PROPERTY_KEY, classificationName).vertices(size);
        if (classificationVertices == null) {
            LOG.info("classificationVertices are null");
            return null;
        }
        return classificationVertices.iterator();
    }

    public static List<AtlasVertex> getAllAssetsWithClassificationVertex(AtlasVertex classificationVertice, int availableSlots) {
        HashSet<AtlasVertex> entityVerticesSet = new HashSet<>();
        Iterator<AtlasVertex> attachedVerticesIterator = null;

        try {
            Iterable attachedVertices = classificationVertice.query()
                    .direction(AtlasEdgeDirection.IN)
                    .label(CLASSIFICATION_LABEL).vertices(availableSlots);
            if (attachedVertices != null) {
                attachedVerticesIterator = attachedVertices.iterator();
                while (attachedVerticesIterator.hasNext()) {
                    entityVerticesSet.add(attachedVerticesIterator.next());
                }
                LOG.info("entityVerticesSet size: {}", entityVerticesSet.size());
            }
        }
        catch (IllegalStateException e){
            e.printStackTrace();
        } finally {
            // Close iterator to release resources
            if (attachedVerticesIterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) attachedVerticesIterator).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }
        return entityVerticesSet.stream().collect(Collectors.toList());
    }

    public static long getAssetsCountOfClassificationVertex(AtlasVertex classificationVertice) {
        long count = 0;
        try {
            count = classificationVertice.query()
                    .direction(AtlasEdgeDirection.IN)
                    .label(CLASSIFICATION_LABEL).count();
        }
        catch (IllegalStateException e){
            e.printStackTrace();
        }
        return count;
    }
    public static AtlasEdge getClassificationEdge(AtlasVertex entityVertex, AtlasVertex classificationVertex) {
        AtlasEdge ret   = null;
        Iterable  edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL)
                                              .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, false)
                                              .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex)).edges();
        if (edges != null) {
            Iterator<AtlasEdge> iterator = edges.iterator();

            if (iterator.hasNext()) {
                AtlasEdge edge = iterator.next();

                ret = (edge != null && edge.getInVertex().equals(classificationVertex)) ? edge : null;
            }
        }

        return ret;
    }

    public static boolean isClassificationAttached(AtlasVertex entityVertex, AtlasVertex classificationVertex) {
        AtlasPerfMetrics.MetricRecorder isClassificationAttachedMetricRecorder  = RequestContext.get().startMetricRecord("isClassificationAttached");
        String                          classificationId                        = classificationVertex.getIdForDisplay();
        try {
            Iterator<AtlasVertex> vertices = entityVertex.query()
                    .direction(AtlasEdgeDirection.OUT)
                    .label(CLASSIFICATION_LABEL)
                    .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex))
                    .vertices().iterator();

            if (vertices != null) {
                while (vertices.hasNext()) {
                    AtlasVertex vertex = vertices.next();
                    if (vertex != null) {
                        if (vertex.getIdForDisplay().equals(classificationId)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception err) {
            throw err;
        } finally {
            RequestContext.get().endMetricRecord(isClassificationAttachedMetricRecorder);
        }
        return false;
    }

    public static AtlasEdge getPropagatedClassificationEdge(AtlasVertex entityVertex, String classificationName, String associatedEntityGuid) {
        AtlasEdge ret   = null;
        Iterable  edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL)
                                              .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, true)
                                              .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, classificationName).edges();
        if (edges != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                while (iterator.hasNext()) {
                    AtlasEdge   edge                 = iterator.next();
                    AtlasVertex classificationVertex = (edge != null) ? edge.getInVertex() : null;

                    if (classificationVertex != null) {
                        String guid = AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_GUID, String.class);

                        if (StringUtils.equals(guid, associatedEntityGuid)) {
                            ret = edge;
                            break;
                        }
                    }
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }

    public static AtlasEdge getPropagatedClassificationEdge(AtlasVertex entityVertex, AtlasVertex classificationVertex) {
        AtlasEdge ret   = null;
        Iterable  edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL)
                                              .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, true)
                                              .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex)).edges();

        if (edges != null && classificationVertex != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                while (iterator != null && iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();

                    if (edge != null && edge.getInVertex().equals(classificationVertex)) {
                        ret = edge;
                        break;
                    }
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }

    public static List<AtlasEdge> getPropagatedEdges(AtlasVertex classificationVertex) {
        List<AtlasEdge> ret   = new ArrayList<>();
        Iterable        edges = classificationVertex.query().direction(AtlasEdgeDirection.IN).label(CLASSIFICATION_LABEL)
                                                    .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, true)
                                                    .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex)).edges();
        if (edges != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                while (iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();

                    ret.add(edge);
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }

    public static List<String> getPropagatedVerticesIds (AtlasVertex classificationVertex) {
        List<String>   ret      =  new ArrayList<>();
        Iterator<AtlasVertex> vertices = null;

        try {
            vertices = classificationVertex.query().direction(AtlasEdgeDirection.IN).label(CLASSIFICATION_LABEL)
                    .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, true)
                    .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex))
                    .vertices().iterator();

            if (vertices != null) {
                while (vertices.hasNext()) {
                    AtlasVertex vertex = vertices.next();
                    if (vertex != null) {
                        ret.add(vertex.getIdForDisplay());
                    }
                }
            }
        } finally {
            // Close iterator to release resources
            if (vertices instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) vertices).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }

        return ret;
    }

    public static boolean hasEntityReferences(AtlasVertex classificationVertex) {
        Iterator edgeIterator = null;
        try {
            edgeIterator = classificationVertex.query().direction(AtlasEdgeDirection.IN).label(CLASSIFICATION_LABEL).edges(1).iterator();
            return edgeIterator != null && edgeIterator.hasNext();
        } finally {
            // Close iterator to release resources
            if (edgeIterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) edgeIterator).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }
    }

    public static List<AtlasVertex> getAllPropagatedEntityVertices(AtlasVertex classificationVertex) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (classificationVertex != null) {
            List<AtlasEdge> edges = getPropagatedEdges(classificationVertex);

            if (CollectionUtils.isNotEmpty(edges)) {
                for (AtlasEdge edge : edges) {
                    ret.add(edge.getOutVertex());
                }
            }
        }

        return ret;
    }

    public static Iterator<AtlasEdge> getIncomingEdgesByLabel(AtlasVertex instanceVertex, String edgeLabel) {
        return getAdjacentEdgesByLabel(instanceVertex, AtlasEdgeDirection.IN, edgeLabel);
    }

    public static Iterator<AtlasEdge> getOutGoingEdgesByLabel(AtlasVertex instanceVertex, String edgeLabel) {
        return getAdjacentEdgesByLabel(instanceVertex, AtlasEdgeDirection.OUT, edgeLabel);
    }

    public static long getOutGoingEdgesCountByLabel(AtlasVertex instanceVertex, String edgeLabel) {
        return getAdjacentEdgesCountByLabel(instanceVertex, AtlasEdgeDirection.OUT, edgeLabel);
    }

    public static long getInComingEdgesCountByLabel(AtlasVertex instanceVertex, String edgeLabel) {
        return getAdjacentEdgesCountByLabel(instanceVertex, AtlasEdgeDirection.IN, edgeLabel);
    }

    public AtlasEdge getEdgeForLabel(AtlasVertex vertex, String edgeLabel, AtlasRelationshipEdgeDirection edgeDirection) {
        AtlasEdge ret;

        switch (edgeDirection) {
            case IN:
            ret = getEdgeForLabel(vertex, edgeLabel, AtlasEdgeDirection.IN);
            break;

            case OUT:
            ret = getEdgeForLabel(vertex, edgeLabel, AtlasEdgeDirection.OUT);
            break;

            case BOTH:
            default:
                ret = getEdgeForLabel(vertex, edgeLabel, AtlasEdgeDirection.BOTH);
                break;
        }

        return ret;
    }

    public static Iterator<AtlasEdge> getEdgesForLabel(AtlasVertex vertex, String edgeLabel, AtlasRelationshipEdgeDirection edgeDirection) {
        RequestContext context = RequestContext.get();
        if (context.isInvokedByIndexSearch() && context.isInvokedByProduct() && AtlasConfiguration.OPTIMISE_SUPER_VERTEX.getBoolean()) {
            return getAdjacentEdgesByLabelWithTimeout(vertex, AtlasEdgeDirection.valueOf(edgeDirection.name()), edgeLabel,
                    AtlasConfiguration.MIN_TIMEOUT_SUPER_VERTEX.getLong());
        }
        return getAdjacentEdgesByLabel(vertex, AtlasEdgeDirection.valueOf(edgeDirection.name()), edgeLabel);
    }



    /**
     * Returns the active edge for the given edge label.
     * If the vertex is deleted and there is no active edge, it returns the latest deleted edge
     * @param vertex
     * @param edgeLabel
     * @return
     */
    public AtlasEdge getEdgeForLabel(AtlasVertex vertex, String edgeLabel) {
        return getEdgeForLabel(vertex, edgeLabel, AtlasEdgeDirection.OUT);
    }

    public AtlasEdge getEdgeForLabel(AtlasVertex vertex, String edgeLabel, AtlasEdgeDirection edgeDirection) {
        Iterator<AtlasEdge> iterator = getAdjacentEdgesByLabel(vertex, edgeDirection, edgeLabel);
        AtlasEdge latestDeletedEdge = null;
        long latestDeletedEdgeTime = Long.MIN_VALUE;

        while (iterator != null && iterator.hasNext()) {
            AtlasEdge edge = iterator.next();
            Id.EntityState edgeState = getState(edge);
            if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found {}", string(edge));
                }

                return edge;
            } else {
                Long modificationTime = edge.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
                if (modificationTime != null && modificationTime >= latestDeletedEdgeTime) {
                    latestDeletedEdgeTime = modificationTime;
                    latestDeletedEdge = edge;
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Found {}", latestDeletedEdge == null ? "null" : string(latestDeletedEdge));
        }

        //If the vertex is deleted, return latest deleted edge
        return latestDeletedEdge;
    }

    public static String vertexString(final AtlasVertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            Collection<?> propertyValues = vertex.getPropertyValues(propertyKey, Object.class);
            properties.append(propertyKey).append("=").append(propertyValues.toString()).append(", ");
        }

        return "v[" + vertex.getIdForDisplay() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final AtlasEdge edge) {
        return "e[" + edge.getLabel() + "], [" + edge.getOutVertex() + " -> " + edge.getLabel() + " -> "
                + edge.getInVertex() + "]";
    }

    private static <T extends AtlasElement> String string(T element) {
        if (element instanceof AtlasVertex) {
            return string((AtlasVertex) element);
        } else if (element instanceof AtlasEdge) {
            return string((AtlasEdge)element);
        }
        return element.toString();
    }

    /**
     * Remove the specified edge from the graph.
     *
     * @param edge
     */
    public void removeEdge(AtlasEdge edge) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeEdge({})", string(edge));
        }

        graph.removeEdge(edge);

        if (LOG.isDebugEnabled()) {
            LOG.info("<== removeEdge()");
        }
    }

    /**
     * Remove the specified AtlasVertex from the graph.
     *
     * @param vertex
     */
    public void removeVertex(AtlasVertex vertex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GraphHelper.removeVertex({})", string(vertex));
        }

        graph.removeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GraphHelper.removeVertex()");
        }
    }

    public AtlasVertex getVertexForGUID(String guid) throws EntityNotFoundException {
        return findVertex(Constants.GUID_PROPERTY_KEY, guid);
    }

    public AtlasEdge getEdgeForGUID(String guid) throws AtlasBaseException {
        AtlasEdge ret;

        try {
            ret = findEdge(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, guid);
        } catch (EntityNotFoundException e) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        return ret;
    }

    /**
     * Finds the Vertices that correspond to the given property values.  Property
     * values that are not found in the graph will not be in the map.
     *
     *  @return propertyValue to AtlasVertex map with the result.
     */
    public Map<String, AtlasVertex> getVerticesForPropertyValues(String property, List<String> values) {

        if(values.isEmpty()) {
            return Collections.emptyMap();
        }
        Collection<String> nonNullValues = new HashSet<>(values.size());

        for(String value : values) {
            if(value != null) {
                nonNullValues.add(value);
            }
        }

        //create graph query that finds vertices with the guids
        AtlasGraphQuery query = graph.query();
        query.in(property, nonNullValues);
        Iterable<AtlasVertex> results = query.vertices();

        Map<String, AtlasVertex> result = new HashMap<>(values.size());
        //Process the result, using the guidToIndexMap to figure out where
        //each vertex should go in the result list.
        Iterator<AtlasVertex> resultIterator = null;
        try {
            resultIterator = results.iterator();

            while (resultIterator.hasNext()) {
                AtlasVertex vertex = resultIterator.next();
                if(vertex.exists()) {
                    String propertyValue = vertex.getProperty(property, String.class);
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Found a vertex {} with {} =  {}", string(vertex), property, propertyValue);
                    }
                    result.put(propertyValue, vertex);
                }
            }
        } finally {
            // Close iterator to release resources
            if (resultIterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) resultIterator).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }
        return result;
    }

    /**
     * Finds the Vertices that correspond to the given GUIDs.  GUIDs
     * that are not found in the graph will not be in the map.
     *
     *  @return GUID to AtlasVertex map with the result.
     */
    public Map<String, AtlasVertex> getVerticesForGUIDs(List<String> guids) {

        return getVerticesForPropertyValues(Constants.GUID_PROPERTY_KEY, guids);
    }

    public static void updateModificationMetadata(AtlasVertex vertex) {
        int maxRetries = 2; // Number of retries
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                AtlasGraphUtilsV2.setEncodedProperty(vertex, MODIFIED_BY_KEY, RequestContext.get().getUser());
                break;
            } catch (Exception e) {
                LOG.error("Attempt : {} , Exception while updating metadata attributes.", attempt, e);
                if (attempt == maxRetries) {
                    throw e;
                }
            }
        }
    }

    public static void updateMetadataAttributes(AtlasVertex vertex, List<String> attributes, String metadataType) {
        if (attributes != null && attributes.size() > 0) {
            for (String attributeName: attributes) {
                if (metadataType.equals("timestamp")) {
                    AtlasGraphUtilsV2.setEncodedProperty(vertex, attributeName, RequestContext.get().getRequestTime());
                } else if (metadataType.equals("user")) {
                    AtlasGraphUtilsV2.setEncodedProperty(vertex, attributeName, RequestContext.get().getUser());
                }
            }
        }
    }

    public static String getQualifiedNameForMapKey(String prefix, String key) {
        return prefix + "." + key;
    }

    public static String getTraitLabel(String typeName, String attrName) {
        return attrName;
    }

    public static String getTraitLabel(String traitName) {
        return traitName;
    }

    public static List<String> handleGetTraitNames(AtlasVertex entityVertex) {
        return handleGetTraitNames(entityVertex, false);
    }

    public static List<String> getPropagatedTraitNames(AtlasVertex entityVertex) {
        return handleGetTraitNames(entityVertex, true);
    }

    public static List<String> getAllTagNames(List<AtlasClassification> tags) {
        return tags.stream().map(AtlasStruct::getTypeName).collect(Collectors.toList());
    }
    public static List<String> getAllTraitNames(AtlasVertex entityVertex) {
        return handleGetTraitNames(entityVertex, null);
    }

    public static List<String> handleGetTraitNames(AtlasVertex entityVertex, Boolean propagated) {
        if (DynamicConfigStore.isTagV2Enabled()) {
            return getTraitNamesV2(entityVertex, propagated);
        } else {
            return getTraitNamesV1(entityVertex, propagated);
        }
    }

    public static List<String> getTraitNamesV1(AtlasVertex entityVertex, Boolean propagated) {
        List<String>     ret   = new ArrayList<>();
        AtlasVertexQuery query = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL);

        if (propagated != null) {
            query = query.has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, propagated);
        }

        Iterable edges = query.edges();

        if (edges != null) {
            Iterator<AtlasEdge> iterator = edges.iterator();

            while (iterator.hasNext()) {
                AtlasEdge edge = iterator.next();

                ret.add(AtlasGraphUtilsV2.getEncodedProperty(edge, CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class));
            }
        }

        return ret;
    }

    public static List<String> getTraitNamesV2(AtlasVertex entityVertex, Boolean propagated) {
        List<String>     ret   = new ArrayList<>();
        try {
            TagDAO tagDAOCassandra = TagDAOCassandraImpl.getInstance();
            if (!propagated) {
                ret = tagDAOCassandra.getAllDirectClassificationsForVertex(entityVertex.getIdForDisplay())
                                     .stream()
                                     .map(AtlasClassification::getTypeName)
                                     .collect(Collectors.toList());
            } else {
                ret = tagDAOCassandra.findByVertexIdAndPropagated(entityVertex.getIdForDisplay())
                                     .stream()
                                     .map(AtlasClassification::getTypeName)
                                     .collect(Collectors.toList());
            }
        } catch (AtlasBaseException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static List<AtlasVertex> getPropagatableClassifications(AtlasEdge edge) {
        List<AtlasVertex> ret = new ArrayList<>();

        RequestContext requestContext = RequestContext.get();

        if ((edge != null && getStatus(edge) != DELETED) || requestContext.getCurrentTask() != null) {
            PropagateTags propagateTags = getPropagateTags(edge);
            AtlasVertex   outVertex     = edge.getOutVertex();
            AtlasVertex   inVertex      = edge.getInVertex();

            if (propagateTags == PropagateTags.ONE_TO_TWO || propagateTags == PropagateTags.BOTH) {
                ret.addAll(getPropagationEnabledClassificationVertices(outVertex));
            }

            if (propagateTags == PropagateTags.TWO_TO_ONE || propagateTags == PropagateTags.BOTH) {
                ret.addAll(getPropagationEnabledClassificationVertices(inVertex));
            }
        }

        return ret;
    }

    public static List<AtlasClassification> getPropagatableClassificationsV2(AtlasEdge edge) throws AtlasBaseException {
        List<AtlasClassification> ret = new ArrayList<>(0);

        if ((edge != null && getStatus(edge) != DELETED) || RequestContext.get().getCurrentTask() != null) {
            AtlasVertex vertex = getPropagatingVertex(edge);
            if (vertex != null) {
                List<AtlasClassification> allTags = TagDAOCassandraImpl.getInstance().getAllClassificationsForVertex(vertex.getIdForDisplay());

                ret = allTags.stream()
                        .filter(Objects::nonNull)
                        .filter(x -> x.getPropagate() != null && x.getPropagate())
                        .toList();
            }
        }

        return ret;
    }

    //Returns the vertex from which the tag is being propagated
    public static AtlasVertex getPropagatingVertex(AtlasEdge edge) {
        if(edge != null) {
            PropagateTags propagateTags = getPropagateTags(edge);
            AtlasVertex   outVertex     = edge.getOutVertex();
            AtlasVertex   inVertex      = edge.getInVertex();

            if (propagateTags == PropagateTags.ONE_TO_TWO || propagateTags == PropagateTags.BOTH) {
                return outVertex;
            }

            if (propagateTags == PropagateTags.TWO_TO_ONE || propagateTags == PropagateTags.BOTH) {
                return inVertex;
            }

        }
        return null;
    }

    public static List<AtlasVertex> getPropagationEnabledClassificationVertices(AtlasVertex entityVertex) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("getPropagationEnabledClassificationVertices");
        List<AtlasVertex> ret   = new ArrayList<>();
        if (entityVertex.hasEdges(AtlasEdgeDirection.OUT, CLASSIFICATION_LABEL)) {
            Iterable edges = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL).edges();

            if (edges != null) {
                Iterator<AtlasEdge> iterator = edges.iterator();

                while (iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();

                    if (edge != null) {
                        AtlasVertex classificationVertex = edge.getInVertex();

                        if (isPropagationEnabled(classificationVertex)) {
                            ret.add(classificationVertex);
                        }
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    public static boolean propagatedClassificationAttachedToVertex(AtlasVertex classificationVertex, AtlasVertex entityVertex) {
        Iterator<AtlasVertex> classificationVertices =  entityVertex.query().direction(AtlasEdgeDirection.OUT)
                .label(CLASSIFICATION_LABEL)
                .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex))
                .vertices().iterator();

        while (classificationVertices.hasNext()) {
            String _classificationVertexId = classificationVertices.next().getIdForDisplay();
            if (_classificationVertexId.equals(classificationVertex.getIdForDisplay())) {
                return true;
            }
        }
        return false;
    }

    public static List<AtlasEdge> getClassificationEdges(AtlasVertex entityVertex) {
        return getClassificationEdges(entityVertex, false, null);
    }

    public static List<AtlasEdge> getPropagatedClassificationEdges(AtlasVertex entityVertex) {
        return getClassificationEdges(entityVertex, true, null);
    }

    public static List<AtlasEdge> getAllClassificationEdges(AtlasVertex entityVertex) {
        return getClassificationEdges(entityVertex, null, null);
    }

    public static List<AtlasEdge> getClassificationEdges(AtlasVertex entityVertex, Boolean propagated, String typeName) {
        List<AtlasEdge>  ret   = new ArrayList<>();
        AtlasVertexQuery query = entityVertex.query().direction(AtlasEdgeDirection.OUT).label(CLASSIFICATION_LABEL);

        if (propagated != null) {
            query = query.has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, propagated);
        }

        if (StringUtils.isNotEmpty(typeName)) {
            query = query.has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, typeName);
        }

        Iterable edges = query.edges();

        if (edges != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                while (iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();

                    if (edge != null) {
                        ret.add(edge);
                    }
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }

    public static List<String> getSuperTypeNames(AtlasVertex<?,?> entityVertex) {
        ArrayList<String>  superTypes     = new ArrayList<>();
        Collection<String> propertyValues = entityVertex.getPropertyValues(SUPER_TYPES_PROPERTY_KEY, String.class);

        if (CollectionUtils.isNotEmpty(propertyValues)) {
            for(String value : propertyValues) {
                superTypes.add(value);
            }
        }

        return superTypes;
    }

    public static String getEdgeLabel(AtlasAttribute aInfo) throws AtlasException {
        return aInfo.getRelationshipEdgeLabel();
    }

    public static Id getIdFromVertex(String dataTypeName, AtlasVertex vertex) {
        return new Id(getGuid(vertex),
                getVersion(vertex).intValue(), dataTypeName,
                getStateAsString(vertex));
    }

    public static Id getIdFromVertex(AtlasVertex vertex) {
        return getIdFromVertex(getTypeName(vertex), vertex);
    }

    public static String getRelationshipGuid(AtlasElement element) {
        return element.getProperty(Constants.RELATIONSHIP_GUID_PROPERTY_KEY, String.class);
    }

    public static String getGuid(AtlasVertex vertex) {
        Object vertexId = vertex.getId();
        String ret = GraphTransactionInterceptor.getVertexGuidFromCache(vertexId);

        if (ret == null) {
            ret = vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY, String.class);

            GraphTransactionInterceptor.addToVertexGuidCache(vertexId, ret);
        }

        return ret;
    }

    public static Map<String,String> fetchAttributes(AtlasVertex vertex, List<String> attributes) {
        Map<String,String> attributesList = new HashMap<>();

        for (String attr: attributes){
            if (Objects.equals(attr, ATTRIBUTE_NAME_GUID)){
                // always add guid to the list from cache
                attributesList.put(ATTRIBUTE_NAME_GUID, getGuid(vertex));
            }
            else{
                attributesList.put(attr, vertex.<String>getProperty(attr, String.class));
            }
        }
        // Return the ArrayList
        return attributesList;
    }

    public static String getHomeId(AtlasElement element) {
        return element.getProperty(Constants.HOME_ID_KEY, String.class);
    }

    public static Boolean isProxy(AtlasElement element) {
        return element.getProperty(Constants.IS_PROXY_KEY, Boolean.class);
    }

    public static Boolean isEntityIncomplete(AtlasElement element) {
        Integer value = element.getProperty(Constants.IS_INCOMPLETE_PROPERTY_KEY, Integer.class);
        Boolean ret   = value != null && value.equals(INCOMPLETE_ENTITY_VALUE) ? Boolean.TRUE : Boolean.FALSE;

        return ret;
    }

    public static Boolean getEntityHasLineage(AtlasElement element) {
        if (element.getPropertyKeys().contains(HAS_LINEAGE)) {
            return element.getProperty(HAS_LINEAGE, Boolean.class);
        } else {
            return false;
        }
    }

    public static Boolean getEntityHasLineageValid(AtlasElement element) {
        if (element.getPropertyKeys().contains(HAS_LINEAGE_VALID)) {
            return element.getProperty(HAS_LINEAGE_VALID, Boolean.class);
        } else {
            return false;
        }
    }

    public static Map getCustomAttributes(AtlasElement element) {
        Map    ret               = null;
        String customAttrsString = element.getProperty(CUSTOM_ATTRIBUTES_PROPERTY_KEY, String.class);

        if (customAttrsString != null) {
            ret = AtlasType.fromJson(customAttrsString, Map.class);
        }

        return ret;
    }

    public static Set<String> getLabels(AtlasElement element) {
        return parseLabelsString(element.getProperty(LABELS_PROPERTY_KEY, String.class));
    }

    public static Integer getProvenanceType(AtlasElement element) {
        return element.getProperty(Constants.PROVENANCE_TYPE_KEY, Integer.class);
    }

    public static String getTypeName(AtlasElement element) {
        return element.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public static Id.EntityState getState(AtlasElement element) {
        String state = getStateAsString(element);
        return state == null ? null : Id.EntityState.valueOf(state);
    }

    public static Long getVersion(AtlasElement element) {
        return element.getProperty(Constants.VERSION_PROPERTY_KEY, Long.class);
    }

    public static String getStateAsString(AtlasElement element) {
        return element.getProperty(STATE_PROPERTY_KEY, String.class);
    }

    public static Status getStatus(AtlasVertex vertex) {
        Object vertexId = vertex.getId();
        Status ret = GraphTransactionInterceptor.getVertexStateFromCache(vertexId);

        if (ret == null) {
            ret = (getState(vertex) == Id.EntityState.DELETED) ? Status.DELETED : Status.ACTIVE;

            GraphTransactionInterceptor.addToVertexStateCache(vertexId, ret);
        }

        return ret;
    }

    public static Status getStatus(AtlasEdge edge) {
        Object edgeId = edge.getId();
        Status ret = GraphTransactionInterceptor.getEdgeStateFromCache(edgeId);

        if (ret == null) {
            ret = (getState(edge) == Id.EntityState.DELETED) ? Status.DELETED : Status.ACTIVE;

            GraphTransactionInterceptor.addToEdgeStateCache(edgeId, ret);
        }

        return ret;
    }


    public static AtlasRelationship.Status getEdgeStatus(AtlasElement element) {
        return (getState(element) == Id.EntityState.DELETED) ? AtlasRelationship.Status.DELETED : AtlasRelationship.Status.ACTIVE;
    }

    public static String getClassificationName(AtlasVertex classificationVertex) {
        return AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_VERTEX_NAME_KEY, String.class);
    }

    public static String getClassificationEntityGuid(AtlasVertex classificationVertex) {
        return AtlasGraphUtilsV2.getEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_GUID, String.class);
    }

    public static Integer getIndexValue(AtlasEdge edge) {
        Integer index = edge.getProperty(ATTRIBUTE_INDEX_PROPERTY_KEY, Integer.class);
        return (index == null) ? 0: index;
    }

    public static boolean isPropagatedClassificationEdge(AtlasEdge edge) {
        boolean ret = false;

        if (edge != null) {
            Boolean isPropagated = edge.getProperty(Constants.CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class);

            if (isPropagated != null) {
                ret = isPropagated.booleanValue();
            }
        }

        return ret;
    }

    public static boolean isClassificationEdge(AtlasEdge edge) {
        boolean ret = false;

        if (edge != null) {
            String  edgeLabel    = edge.getLabel();
            Boolean isPropagated = edge.getProperty(Constants.CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class);

            if (edgeLabel != null && isPropagated != null) {
                ret = edgeLabel.equals(CLASSIFICATION_LABEL) && !isPropagated;
            }
        }

        return ret;
    }

    public static List<String> getBlockedClassificationIds(AtlasEdge edge) {
        List<String> ret = null;

        if (edge != null) {
            List<String> classificationIds = AtlasGraphUtilsV2.getEncodedProperty(edge, RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY, List.class);

            ret = CollectionUtils.isNotEmpty(classificationIds) ? classificationIds : Collections.emptyList();
        }

        return ret;
    }

    public static PropagateTags getPropagateTags(AtlasElement element) {
        String propagateTags = element.getProperty(RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, String.class);

        return (propagateTags == null) ? null : PropagateTags.valueOf(propagateTags);
    }

    public static Status getClassificationEntityStatus(AtlasElement element) {
        String status = element.getProperty(CLASSIFICATION_ENTITY_STATUS, String.class);

        return (status == null) ? null : Status.valueOf(status);
    }

    //Added conditions in fetching system attributes to handle test failures in GremlinTest where these properties are not set
    public static String getCreatedByAsString(AtlasElement element){
        return element.getProperty(CREATED_BY_KEY, String.class);
    }

    public static String getModifiedByAsString(AtlasElement element){
        return element.getProperty(MODIFIED_BY_KEY, String.class);
    }

    public static void setModifiedByAsString(AtlasElement element, String modifiedBy){
         element.setProperty(MODIFIED_BY_KEY, modifiedBy);
    }

    public static long getCreatedTime(AtlasElement element){
        try {
            return element.getProperty(TIMESTAMP_PROPERTY_KEY, Long.class);
        } catch (Exception e) {
            LOG.warn("Failed to get created time for vertex {}. Error: {}", element.getIdForDisplay(), e.getMessage());
            return 0l;
        }
    }

    public static long getModifiedTime(AtlasElement element){
        try {
            return element.getProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
        } catch (Exception e) {
            LOG.warn("Failed to get modified time for vertex {}. Error: {}", element.getIdForDisplay(), e.getMessage());
            return getCreatedTime(element);
        }
    }

    public static void setModifiedTime(AtlasElement element, Long modifiedTime) {
        element.setProperty(MODIFICATION_TIMESTAMP_PROPERTY_KEY, modifiedTime);
    }

    public static boolean isActive(AtlasEntity entity) {
        return entity != null ? entity.getStatus() == ACTIVE : false;
    }

    /**
     * For the given type, finds an unique attribute and checks if there is an existing instance with the same
     * unique value
     *
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    public AtlasVertex getVertexForInstanceByUniqueAttribute(AtlasEntityType classType, Referenceable instance)
        throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        }

        AtlasVertex result = null;
        for (AtlasAttribute attributeInfo : classType.getUniqAttributes().values()) {
            String propertyKey = attributeInfo.getQualifiedName();
            try {
                result = findVertex(propertyKey, instance.get(attributeInfo.getName()),
                        ENTITY_TYPE_PROPERTY_KEY, classType.getTypeName(),
                        STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found vertex by unique attribute : {}={}", propertyKey, instance.get(attributeInfo.getName()));
                }
            } catch (EntityNotFoundException e) {
                //Its ok if there is no entity with the same unique value
            }
        }

        return result;
    }

    /**
     * Finds vertices that match at least one unique attribute of the instances specified.  The AtlasVertex at a given index in the result corresponds
     * to the IReferencableInstance at that same index that was passed in.  The number of elements in the resultant list is guaranteed to match the
     * number of instances that were passed in.  If no vertex is found for a given instance, that entry will be null in the resultant list.
     *
     *
     * @param classType
     * @param instancesForClass
     * @return
     * @throws AtlasException
     */
    public List<AtlasVertex> getVerticesForInstancesByUniqueAttribute(AtlasEntityType classType, List<? extends Referenceable> instancesForClass) throws AtlasException {

        //For each attribute, need to figure out what values to search for and which instance(s)
        //those values correspond to.
        Map<String, AttributeValueMap> map = new HashMap<String, AttributeValueMap>();

        for (AtlasAttribute attributeInfo : classType.getUniqAttributes().values()) {
            String propertyKey = attributeInfo.getQualifiedName();
            AttributeValueMap mapForAttribute = new AttributeValueMap();
            for(int idx = 0; idx < instancesForClass.size(); idx++) {
                Referenceable instance = instancesForClass.get(idx);
                Object value = instance.get(attributeInfo.getName());
                mapForAttribute.put(value, instance, idx);
            }
            map.put(propertyKey, mapForAttribute);
        }

        AtlasVertex[] result = new AtlasVertex[instancesForClass.size()];
        if(map.isEmpty()) {
            //no unique attributes
            return Arrays.asList(result);
        }

        //construct gremlin query
        AtlasGraphQuery query = graph.query();

        query.has(ENTITY_TYPE_PROPERTY_KEY, classType.getTypeName());
        query.has(STATE_PROPERTY_KEY,Id.EntityState.ACTIVE.name());

        List<AtlasGraphQuery> orChildren = new ArrayList<AtlasGraphQuery>();


        //build up an or expression to find vertices which match at least one of the unique attribute constraints
        //For each unique attributes, we add a within clause to match vertices that have a value of that attribute
        //that matches the value in some instance.
        for(Map.Entry<String, AttributeValueMap> entry : map.entrySet()) {
            AtlasGraphQuery orChild = query.createChildQuery();
            String propertyName = entry.getKey();
            AttributeValueMap valueMap = entry.getValue();
            Set<Object> values = valueMap.getAttributeValues();
            if(values.size() == 1) {
                orChild.has(propertyName, values.iterator().next());
            }
            else if(values.size() > 1) {
                orChild.in(propertyName, values);
            }
            orChildren.add(orChild);
        }

        if(orChildren.size() == 1) {
            AtlasGraphQuery child = orChildren.get(0);
            query.addConditionsFrom(child);
        }
        else if(orChildren.size() > 1) {
            query.or(orChildren);
        }

        Iterable<AtlasVertex> queryResult = query.vertices();

        Iterator<AtlasVertex> resultIterator = null;
        try {
            resultIterator = queryResult.iterator();

            while (resultIterator.hasNext()) {
                AtlasVertex matchingVertex = resultIterator.next();
                Collection<IndexedInstance> matches = getInstancesForVertex(map, matchingVertex);
                for(IndexedInstance wrapper : matches) {
                    result[wrapper.getIndex()]= matchingVertex;
                }
            }
        } finally {
            // Close iterator to release resources
            if (resultIterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) resultIterator).close();
                } catch (Exception e) {
                    LOG.debug("Error closing iterator resources", e);
                }
            }
        }
        return Arrays.asList(result);
    }

    //finds the instance(s) that correspond to the given vertex
    private Collection<IndexedInstance> getInstancesForVertex(Map<String, AttributeValueMap> map, AtlasVertex foundVertex) {

        //loop through the unique attributes.  For each attribute, check to see if the vertex property that
        //corresponds to that attribute has a value from one or more of the instances that were passed in.

        for(Map.Entry<String, AttributeValueMap> entry : map.entrySet()) {

            String propertyName = entry.getKey();
            AttributeValueMap valueMap = entry.getValue();

            Object vertexValue = foundVertex.getProperty(propertyName, Object.class);

            Collection<IndexedInstance> instances = valueMap.get(vertexValue);
            if(instances != null && instances.size() > 0) {
                //return first match.  Let the underling graph determine if this is a problem
                //(i.e. if the other unique attributes change be changed safely to match what
                //the user requested).
                return instances;
            }
            //try another attribute
        }
        return Collections.emptyList();
    }

    public static List<String> getTypeNames(List<AtlasVertex> vertices) {
        List<String> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(vertices)) {
            for (AtlasVertex vertex : vertices) {
                String entityTypeProperty = vertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);

                if (entityTypeProperty != null) {
                    ret.add(getTypeName(vertex));
                }
            }
        }

        return ret;
    }

    public static AtlasVertex getAssociatedEntityVertex(AtlasVertex classificationVertex) {
        AtlasVertex ret   = null;
        Iterable    edges = classificationVertex.query().direction(AtlasEdgeDirection.IN).label(CLASSIFICATION_LABEL)
                                                .has(CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, false)
                                                .has(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, getTypeName(classificationVertex)).edges();
        if (edges != null) {
            Iterator<AtlasEdge> iterator = null;
            try {
                iterator = edges.iterator();

                if (iterator != null && iterator.hasNext()) {
                    AtlasEdge edge = iterator.next();

                    ret = edge.getOutVertex();
                }
            } finally {
                // Close iterator to release resources
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator resources", e);
                    }
                }
            }
        }

        return ret;
    }

    public AtlasGraph getGraph() {
        return this.graph;
    }

    public Boolean getDefaultRemovePropagations() {
        return this.removePropagations;
    }

    /**
     * Guid and AtlasVertex combo
     */
    public static class VertexInfo {
        private final AtlasEntityHeader entity;
        private final AtlasVertex       vertex;

        public VertexInfo(AtlasEntityHeader entity, AtlasVertex vertex) {
            this.entity = entity;
            this.vertex = vertex;
        }

        public AtlasEntityHeader getEntity() { return entity; }
        public AtlasVertex getVertex() {
            return vertex;
        }
        public String getGuid() {
            return entity.getGuid();
        }
        public String getTypeName() {
            return entity.getTypeName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VertexInfo that = (VertexInfo) o;
            return Objects.equals(entity, that.entity) &&
                    Objects.equals(vertex, that.vertex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entity, vertex);
        }
    }

    public static boolean isInternalType(AtlasVertex vertex) {
        return vertex != null && isInternalType(getTypeName(vertex));
    }

    public static boolean isInternalType(String typeName) {
        return typeName != null && typeName.startsWith(Constants.INTERNAL_PROPERTY_KEY_PREFIX);
    }

    @SuppressWarnings("unchecked,rawtypes")
    public static List<Object> getArrayElementsProperty(AtlasType elementType, AtlasVertex instanceVertex, AtlasAttribute attribute, VertexEdgePropertiesCache vertexEdgePropertiesCache) {
        String propertyName = attribute.getVertexPropertyName();
        boolean isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
        boolean isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);

        if (isReference(elementType)) {
            boolean isStruct = TypeCategory.STRUCT == attribute.getDefinedInType().getTypeCategory() ||
                               TypeCategory.STRUCT == elementType.getTypeCategory();

            if (isStruct) {
                String edgeLabel = AtlasGraphUtilsV2.getEdgeLabel(attribute.getName());
                return (List) getCollectionElementsUsingRelationship(instanceVertex, attribute, edgeLabel);
            } else {
                if( vertexEdgePropertiesCache != null) {
                    List values = vertexEdgePropertiesCache.getCollectionElementsUsingRelationship(instanceVertex.getIdForDisplay(), attribute);
                    return values;
                }
                return (List) getCollectionElementsUsingRelationship(instanceVertex, attribute);
            }
        } else if (isArrayOfPrimitiveType || isArrayOfEnum) {
            if (vertexEdgePropertiesCache != null) {
                List values =  vertexEdgePropertiesCache.getMultiValuedProperties(instanceVertex.getIdForDisplay(), propertyName, elementType.getClass());
                return values;
            }
            return (List) instanceVertex.getMultiValuedProperty(propertyName, elementType.getClass());
        } else {
            return (List) instanceVertex.getListProperty(propertyName);
        }
    }

    public static Map<String, Object> getMapElementsProperty(AtlasMapType mapType, AtlasVertex instanceVertex, String propertyName, AtlasAttribute attribute) {
        AtlasType mapValueType = mapType.getValueType();

        if (isReference(mapValueType)) {
            return getReferenceMap(instanceVertex, attribute, propertyName);
        } else {
            return (Map) instanceVertex.getProperty(propertyName, Map.class);
        }
    }

    // map elements for reference types - AtlasObjectId, AtlasStruct
    public static Map<String, Object> getReferenceMap(AtlasVertex instanceVertex, AtlasAttribute attribute) {
        Map<String, Object> ret            = new HashMap<>();
        List<AtlasEdge>     referenceEdges = getCollectionElementsUsingRelationship(instanceVertex, attribute);

        for (AtlasEdge edge : referenceEdges) {
            String key = edge.getProperty(ATTRIBUTE_KEY_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(key)) {
                ret.put(key, edge);
            }
        }

        return ret;
    }

    public static Map<String, Object> getReferenceMap(AtlasVertex instanceVertex, AtlasAttribute attribute, String propertyName) {
        Map<String, Object> ret            = new HashMap<>();
        List<AtlasEdge>     referenceEdges = getCollectionElementsUsingRelationship(instanceVertex, attribute, propertyName);

        for (AtlasEdge edge : referenceEdges) {
            String key = edge.getProperty(ATTRIBUTE_KEY_PROPERTY_KEY, String.class);

            if (StringUtils.isNotEmpty(key)) {
                ret.put(key, edge);
            }
        }

        return ret;
    }

    public static List<AtlasEdge> getMapValuesUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute) {
        String                         edgeLabel     = attribute.getRelationshipEdgeLabel();
        AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();
        Iterator<AtlasEdge>            edgesForLabel = getEdgesForLabel(vertex, edgeLabel, edgeDirection);

        return (List<AtlasEdge>) IteratorUtils.toList(edgesForLabel);
    }

    // map elements for primitive types
    public static Map<String, Object> getPrimitiveMap(AtlasVertex instanceVertex, String propertyName) {
        Map<String, Object> ret = instanceVertex.getProperty(AtlasGraphUtilsV2.encodePropertyKey(propertyName), Map.class);

        return ret;
    }

    public static List<AtlasEdge> getCollectionElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute) {
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        return getCollectionElementsUsingRelationship(vertex, attribute, edgeLabel);
    }

    public static List<AtlasEdge> getActiveCollectionElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute) throws AtlasBaseException {
        String edgeLabel = attribute.getRelationshipEdgeLabel();
        return getActiveCollectionElementsUsingRelationship(vertex, attribute, edgeLabel);
    }

    public static List<AtlasEdge> getCollectionElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute,
                                                                         boolean isStructType) {
        String edgeLabel = isStructType ? AtlasGraphUtilsV2.getEdgeLabel(attribute.getName()) :  attribute.getRelationshipEdgeLabel();
        return getCollectionElementsUsingRelationship(vertex, attribute, edgeLabel);
    }

    public static List<AtlasEdge> getActiveCollectionElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute, String edgeLabel) throws AtlasBaseException {
        List<AtlasEdge>                ret;
        AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();
        Iterator<AtlasEdge>            edgesForLabel = getActiveEdges(vertex, edgeLabel, AtlasEdgeDirection.valueOf(edgeDirection.name()));

        ret = IteratorUtils.toList(edgesForLabel);

        sortCollectionElements(attribute, ret);

        return ret;
    }


    public static List<AtlasEdge> getCollectionElementsUsingRelationship(AtlasVertex vertex, AtlasAttribute attribute, String edgeLabel) {
        List<AtlasEdge>                ret;
        AtlasRelationshipEdgeDirection edgeDirection = attribute.getRelationshipEdgeDirection();
        Iterator<AtlasEdge>            edgesForLabel = getEdgesForLabel(vertex, edgeLabel, edgeDirection);

        ret = IteratorUtils.toList(edgesForLabel);

        sortCollectionElements(attribute, ret);

        return ret;
    }

    private static void sortCollectionElements(AtlasAttribute attribute, List<AtlasEdge> edges) {
        // sort array elements based on edge index
        if (attribute.getAttributeType() instanceof AtlasArrayType &&
                CollectionUtils.isNotEmpty(edges) &&
                edges.get(0).getProperty(ATTRIBUTE_INDEX_PROPERTY_KEY, Integer.class) != null) {
            Collections.sort(edges, (e1, e2) -> {
                Integer e1Index = getIndexValue(e1);
                Integer e2Index = getIndexValue(e2);

                return e1Index.compareTo(e2Index);
            });
        }
    }

    public static void dumpToLog(final AtlasGraph<?,?> graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (AtlasVertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (AtlasEdge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }

    public static String string(Referenceable instance) {
        return String.format("entity[type=%s guid=%s]", instance.getTypeName(), instance.getId()._getId());
    }

    public static String string(AtlasVertex<?,?> vertex) {
        if(vertex == null) {
            return "vertex[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getVertexDetails(vertex);
            } else {
                return String.format("vertex[id=%s]", vertex.getIdForDisplay());
            }
        }
    }

    public static String getVertexDetails(AtlasVertex<?,?> vertex) {

        return String.format("vertex[id=%s type=%s guid=%s]", vertex.getIdForDisplay(), getTypeName(vertex),
                getGuid(vertex));
    }


    public static String string(AtlasEdge<?,?> edge) {
        if(edge == null) {
            return "edge[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getEdgeDetails(edge);
            } else {
                return String.format("edge[id=%s]", edge.getIdForDisplay());
            }
        }
    }

    public static String getEdgeDetails(AtlasEdge<?,?> edge) {

        return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getIdForDisplay(), edge.getLabel(),
                string(edge.getOutVertex()), string(edge.getInVertex()));
    }

    @VisibleForTesting
    //Keys copied from com.thinkaurelius.titan.graphdb.types.StandardRelationTypeMaker
    //Titan checks that these chars are not part of any keys. So, encoding...
    public static BiMap<String, String> RESERVED_CHARS_ENCODE_MAP =
            HashBiMap.create(new HashMap<String, String>() {{
                put("{", "_o");
                put("}", "_c");
                put("\"", "_q");
                put("$", "_d");
                put("%", "_p");
            }});


    public static String decodePropertyKey(String key) {
        if (StringUtils.isBlank(key)) {
            return key;
        }

        for (String encodedStr : RESERVED_CHARS_ENCODE_MAP.values()) {
            key = key.replace(encodedStr, RESERVED_CHARS_ENCODE_MAP.inverse().get(encodedStr));
        }
        return key;
    }

    public Object getVertexId(String guid) throws EntityNotFoundException {
        AtlasVertex instanceVertex   = getVertexForGUID(guid);
        Object      instanceVertexId = instanceVertex.getId();

        return instanceVertexId;
    }

    /*
    public static AttributeInfo getAttributeInfoForSystemAttributes(String field) {
        switch (field) {
        case Constants.STATE_PROPERTY_KEY:
        case Constants.GUID_PROPERTY_KEY:
        case Constants.CREATED_BY_KEY:
        case Constants.MODIFIED_BY_KEY:
                return TypesUtil.newAttributeInfo(field, DataTypes.STRING_TYPE);

        case Constants.TIMESTAMP_PROPERTY_KEY:
        case Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY:
            return TypesUtil.newAttributeInfo(field, DataTypes.DATE_TYPE);
        }
        return null;
    }
    */

    public static boolean elementExists(AtlasElement v) {
        return v != null && v.exists();
    }

    public static void setListPropertyFromElementIds(AtlasVertex<?, ?> instanceVertex, String propertyName,
            List<AtlasElement> elements) {
        String actualPropertyName = AtlasGraphUtilsV2.encodePropertyKey(propertyName);
        instanceVertex.setPropertyFromElementsIds(actualPropertyName, elements);

    }

    public static void setPropertyFromElementId(AtlasVertex<?, ?> instanceVertex, String propertyName,
            AtlasElement value) {
        String actualPropertyName = AtlasGraphUtilsV2.encodePropertyKey(propertyName);
        instanceVertex.setPropertyFromElementId(actualPropertyName, value);

    }

    public static void setListProperty(AtlasVertex instanceVertex, String propertyName, ArrayList<String> value) {
        String actualPropertyName = AtlasGraphUtilsV2.encodePropertyKey(propertyName);
        instanceVertex.setListProperty(actualPropertyName, value);
    }

    public static List<String> getListProperty(AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = AtlasGraphUtilsV2.encodePropertyKey(propertyName);
        return instanceVertex.getListProperty(actualPropertyName);
    }


    private String getConditionString(Object[] args) {
        StringBuilder condition = new StringBuilder();

        for (int i = 0; i < args.length; i+=2) {
            condition.append(args[i]).append(" = ").append(args[i+1]).append(", ");
        }

        return condition.toString();
    }

    /**
     * Get relationshipDef name from entityType using relationship attribute.
     * if more than one relationDefs are returned for an attribute.
     * e.g. hive_column.table
     *
     * hive_table.columns       -> hive_column.table
     * hive_table.partitionKeys -> hive_column.table
     *
     * resolve by comparing all incoming edges typename with relationDefs name returned for an attribute
     * to pick the right relationshipDef name
     */
    public String getRelationshipTypeName(AtlasVertex entityVertex, AtlasEntityType entityType, String attributeName) {
        String      ret               = null;
        Set<String> relationshipTypes = entityType.getAttributeRelationshipTypes(attributeName);

        if (CollectionUtils.isNotEmpty(relationshipTypes)) {
            if (relationshipTypes.size() == 1) {
                ret = relationshipTypes.iterator().next();
            } else {
                Iterator<AtlasEdge> iter = entityVertex.getEdges(AtlasEdgeDirection.IN).iterator();

                while (iter.hasNext() && ret == null) {
                    String edgeTypeName = AtlasGraphUtilsV2.getTypeName(iter.next());

                    if (relationshipTypes.contains(edgeTypeName)) {
                        ret = edgeTypeName;

                        break;
                    }
                }

                if (ret == null) {
                    //relationshipTypes will have at least one relationshipDef
                    ret = relationshipTypes.iterator().next();
                }
            }
        }

        return ret;
    }

    //get entity type of relationship (End vertex entity type) from relationship label
    public static String getReferencedEntityTypeName(AtlasVertex entityVertex, String relation) {
        String ret = null;
        Iterator<AtlasEdge> edges    = GraphHelper.getAdjacentEdgesByLabel(entityVertex, AtlasEdgeDirection.BOTH, relation);

        if (edges != null && edges.hasNext()) {
            AtlasEdge   relationEdge = edges.next();
            AtlasVertex outVertex    = relationEdge.getOutVertex();
            AtlasVertex inVertex     = relationEdge.getInVertex();

            if (outVertex != null && inVertex != null) {
                String outVertexId    = outVertex.getIdForDisplay();
                String entityVertexId = entityVertex.getIdForDisplay();
                AtlasVertex endVertex = StringUtils.equals(outVertexId, entityVertexId) ? inVertex : outVertex;
                ret                   = GraphHelper.getTypeName(endVertex);
            }
        }

       return ret;
    }

    public static boolean isRelationshipEdge(AtlasEdge edge) {
        if (edge == null) {
            return false;
        }

        String edgeLabel = edge.getLabel();

        return StringUtils.isNotEmpty(edge.getLabel()) ? edgeLabel.startsWith("r:") : false;
    }

    public static AtlasObjectId getReferenceObjectId(AtlasEdge edge, AtlasRelationshipEdgeDirection relationshipDirection,
                                                     AtlasVertex parentVertex) {
        AtlasObjectId ret = null;

        if (relationshipDirection == OUT) {
            ret = getAtlasObjectIdForInVertex(edge);
        } else if (relationshipDirection == IN) {
            ret = getAtlasObjectIdForOutVertex(edge);
        } else if (relationshipDirection == BOTH){
            // since relationship direction is BOTH, edge direction can be inward or outward
            // compare with parent entity vertex and pick the right reference vertex
            if (verticesEquals(parentVertex, edge.getOutVertex())) {
                ret = getAtlasObjectIdForInVertex(edge);
            } else {
                ret = getAtlasObjectIdForOutVertex(edge);
            }
        }

        return ret;
    }

    public static AtlasObjectId getAtlasObjectIdForOutVertex(AtlasEdge edge) {
        return new AtlasObjectId(getGuid(edge.getOutVertex()), getTypeName(edge.getOutVertex()));
    }

    public static AtlasObjectId getAtlasObjectIdForInVertex(AtlasEdge edge) {
        return new AtlasObjectId(getGuid(edge.getInVertex()), getTypeName(edge.getInVertex()));
    }

    private static boolean verticesEquals(AtlasVertex vertexA, AtlasVertex vertexB) {
        return StringUtils.equals(getGuid(vertexB), getGuid(vertexA));
    }

    public static String getDelimitedClassificationNames(Collection<String> classificationNames) {
        String ret = null;

        if (CollectionUtils.isNotEmpty(classificationNames)) {
            ret = CLASSIFICATION_NAME_DELIMITER + StringUtils.join(classificationNames, CLASSIFICATION_NAME_DELIMITER)
                + CLASSIFICATION_NAME_DELIMITER;
        }
        return ret;
    }

    /**
     * Get all the active parents
     * @param vertex entity vertex
     * @param parentEdgeLabel Edge label of parent
     * @return Iterator of children vertices
     */
    public static Iterator<AtlasVertex> getActiveParentVertices(AtlasVertex vertex, String parentEdgeLabel) throws AtlasBaseException {
        return getActiveVertices(vertex, parentEdgeLabel, AtlasEdgeDirection.IN);
    }

    /**
     * Get all the active children of category
     * @param vertex entity vertex
     * @param childrenEdgeLabel Edge label of children
     * @return Iterator of children vertices
     */
    public static Iterator<AtlasVertex> getActiveChildrenVertices(AtlasVertex vertex, String childrenEdgeLabel) throws AtlasBaseException {
        return getActiveVertices(vertex, childrenEdgeLabel, AtlasEdgeDirection.OUT);
    }

    /**
     * Get active children vertices with a limit for optimized existence checks
     * @param vertex entity vertex
     * @param childrenEdgeLabel Edge label of children
     * @param limit Maximum number of vertices to retrieve
     * @return Iterator of children vertices (limited)
     */
    public static Iterator<AtlasVertex> getActiveChildrenVertices(AtlasVertex vertex, String childrenEdgeLabel, int limit) throws AtlasBaseException {
        return getActiveVertices(vertex, childrenEdgeLabel, AtlasEdgeDirection.OUT, limit);
    }

    /**
     * Get all the active edges and cap number of edges to avoid excessive processing.
     * @param vertex entity vertex
     * @param childrenEdgeLabel Edge label of children
     * @return Iterator of children edges
     */
    public static Iterator<AtlasEdge> getActiveEdges(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction) throws AtlasBaseException {
        return getActiveEdges(vertex, childrenEdgeLabel, direction, MAX_EDGES_SUPER_VERTEX.getInt(), TIMEOUT_SUPER_VERTEX_FETCH.getLong());
    }
    /***
     * Get all the active edges and cap number of edges to avoid excessive processing.
     * @param vertex
     * @param childrenEdgeLabel
     * @param direction
     * @param limit
     * @return
     * @throws AtlasBaseException
     */
    public static Iterator<AtlasEdge> getActiveEdges(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction, int limit, long timeout) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("GraphHelper.getActiveEdges");

        try {
            return Single.fromCallable(() -> {
                        Iterator<AtlasEdge> it = vertex.query()
                                .direction(direction)
                                .label(childrenEdgeLabel)
                                .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                                .edges(limit)
                                .iterator();

                        List<AtlasEdge> edgeList = new ArrayList<>();
                        while (it.hasNext()) {
                            edgeList.add(it.next());
                            // Optional: cap edge count to avoid excessive processing
                            if (edgeList.size() > limit) {
                                LOG.warn("Super vertex detected: vertex id = {}, edge label = {}, edge count = {}",
                                        vertex.getId(), childrenEdgeLabel, edgeList.size());
                                break;
                            }
                        }

                        return edgeList.iterator();
                    })
                    .timeout(timeout, TimeUnit.SECONDS)
                    .onErrorReturn(throwable -> {
                        if (throwable instanceof TimeoutException) {
                            LOG.warn("Timeout while getting active edges for vertex id: {}", vertex.getId());
                        } else {
                            LOG.error("Error while getting active edges of vertex for edge label: {}, vertex id: {}",
                                    childrenEdgeLabel, vertex.getId(), throwable);
                        }
                        return Collections.emptyIterator();
                    })
                    .subscribeOn(Schedulers.io())
                    .blockingGet();

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public static Iterator<AtlasVertex> getActiveVertices(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("CategoryPreProcessor.getEdges");

        try {
            return vertex.query()
                    .direction(direction)
                    .label(childrenEdgeLabel)
                    .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                    .vertices()
                    .iterator();
        } catch (Exception e) {
            LOG.error("Error while getting active children of category for edge label " + childrenEdgeLabel, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public static Iterator<AtlasVertex> getActiveVertices(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction, int limit) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("GraphHelper.getActiveVerticesWithLimit");

        try {
            return vertex.query()
                    .direction(direction)
                    .label(childrenEdgeLabel)
                    .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                    .vertices(limit)
                    .iterator();
        } catch (Exception e) {
            LOG.error("Error while getting active vertices with limit for edge label " + childrenEdgeLabel, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public static Iterator<AtlasVertex> getAllChildrenVertices(AtlasVertex vertex, String childrenEdgeLabel) throws AtlasBaseException {
        return getAllVertices(vertex, childrenEdgeLabel, AtlasEdgeDirection.OUT);
    }

    public static Iterator<AtlasVertex> getAllVertices(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("CategoryPreProcessor.getEdges");

        try {
            return vertex.query()
                    .direction(direction)
                    .label(childrenEdgeLabel)
                    .vertices()
                    .iterator();
        } catch (Exception e) {
            LOG.error("Error while getting all children of category for edge label " + childrenEdgeLabel, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public static Set<String> parseLabelsString(String labels) {
        Set<String> ret = new HashSet<>();

        if (StringUtils.isNotEmpty(labels)) {
            ret.addAll(Arrays.asList(StringUtils.split(labels, "\\" + LABEL_NAME_DELIMITER)));
        }

        return ret;
    }
    public Set<AbstractMap.SimpleEntry<String,String>> retrieveEdgeLabelsAndTypeName(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("GraphHelper.retrieveEdgeLabelsAndTypeName");
        long timeoutSeconds = org.apache.atlas.AtlasConfiguration.TIMEOUT_SUPER_VERTEX_FETCH.getLong();
        try {
            // Use try-with-resources to ensure stream is properly closed
            try (Stream<Map<String, Object>> stream = ((AtlasJanusGraph) graph).getGraph().traversal()
                    .V(vertex.getId())
                    .bothE()
                    .has(STATE_PROPERTY_KEY, ACTIVE_STATE_VALUE)
                    .project(LABEL_PROPERTY_KEY, TYPE_NAME_PROPERTY_KEY)
                    .by(T.label)
                    .by(TYPE_NAME_PROPERTY_KEY)
                    .dedup()
                    .toStream()) {

                return stream
                        .map(m -> {
                            Object label = m.get(LABEL_PROPERTY_KEY);
                            Object typeName = m.get(TYPE_NAME_PROPERTY_KEY);
                            String labelStr = (label != null) ? label.toString() : "";
                            String typeNameStr = (typeName != null) ? typeName.toString() : "";

                            return new AbstractMap.SimpleEntry<>(labelStr, typeNameStr);
                        })
                        .filter(entry -> !entry.getKey().isEmpty())
                        .distinct()
                        .collect(Collectors.toSet());
            }

        } catch (Exception e) {
            LOG.error("Error while getting labels of active edges", e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    /**
     * Retrieves adjacent edges by label with a timeout. Logs guid, qualifiedName, and reason on error.
     * Timeout is configurable via AtlasConfiguration.TIMEOUT_SUPER_VERTEX_FETCH.
     * Runs on the IO scheduler and returns an empty iterator on error.
     */
    public static Iterator<AtlasEdge> getAdjacentEdgesByLabelWithTimeout(
            AtlasVertex instanceVertex,
            AtlasEdgeDirection direction,
            final String edgeLabel,
            long timeoutSeconds
    ) {
        final String guid = getGuid(instanceVertex);

        return Single.fromCallable(() -> {
                if (instanceVertex != null && edgeLabel != null) {
                    return instanceVertex.getEdges(direction, edgeLabel).iterator();
                } else {
                    return Collections.emptyIterator();
                }
            })
            .timeout(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)
            .subscribeOn(Schedulers.io())
            .onErrorReturn(throwable -> {
                String reason;
                if (throwable instanceof org.janusgraph.core.JanusGraphException) {
                    reason = "JanusGraphException";
                } else if (throwable instanceof java.util.concurrent.TimeoutException) {
                    reason = "Timeout";
                } else {
                    reason = "Other";
                }
                LOG.warn(
                    "getAdjacentEdgesByLabelWithTimeout failed: guid={}, reason={}. Falling back to getActiveEdges.",
                    guid, reason, throwable
                );
                // Fallback: try to get active edges (already limited)
                try {
                    return getActiveEdges(instanceVertex, edgeLabel, direction,
                            AtlasConfiguration.MIN_EDGES_SUPER_VERTEX.getInt(),
                            AtlasConfiguration.MIN_TIMEOUT_SUPER_VERTEX.getLong());
                } catch (Exception fallbackEx) {
                    LOG.warn("Fallback getActiveEdges also failed: guid={}, reason={}", guid, reason, fallbackEx);
                    return Collections.emptyIterator();
                }
            })
            .blockingGet();
    }

    public Set<AtlasVertex> getVertices(Set<Long> vertexIds) {
        if (CollectionUtils.isEmpty(vertexIds)) return Collections.emptySet();
        Set<String> uniqueVertexIds = vertexIds.stream().map(String::valueOf).collect(Collectors.toSet());
        return graph.getVertices(uniqueVertexIds.toArray(new String[0]));
    }
}