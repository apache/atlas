package org.apache.atlas.repository.store.graph.v2.preprocessor.glossary;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTask;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.ACTIVE_STATE_VALUE;
import static org.apache.atlas.repository.Constants.ELASTICSEARCH_PAGINATION_SIZE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.Constants.MEANINGS_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANINGS_TEXT_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.MEANING_NAMES_PROPERTY_KEY;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.from;

public abstract class AbstractGlossaryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGlossaryPreProcessor.class);

    static final boolean DEFERRED_ACTION_ENABLED = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();

    protected static final String ATTR_MEANINGS   = "meanings";
    protected static final String ATTR_CATEGORIES = "categories";

    protected final AtlasTypeRegistry typeRegistry;
    protected final EntityGraphRetriever entityRetriever;
    protected final TaskManagement taskManagement;

    protected EntityDiscoveryService discovery;

    AbstractGlossaryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, TaskManagement taskManagement) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.taskManagement = taskManagement;

        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    public boolean termExists(String termName, String glossaryQName) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("termExists");
        boolean ret = false;

        ret = AtlasGraphUtilsV2.termExists(termName, glossaryQName);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    public void createAndQueueTask(String taskType,
                                   String currentTermName, String updatedTermName,
                                   String termQName, String updatedTermQualifiedName,
                                   AtlasVertex termVertex) {
        String termGuid = GraphHelper.getGuid(termVertex);
        String currentUser = RequestContext.getCurrentUser();
        Map<String, Object> taskParams = MeaningsTask.toParameters(currentTermName, updatedTermName, termQName, updatedTermQualifiedName, termGuid);
        AtlasTask task = taskManagement.createTask(taskType, currentUser, taskParams);

        AtlasGraphUtilsV2.addEncodedProperty(termVertex, PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    public boolean checkEntityTermAssociation(String termQName) throws AtlasBaseException {
        List<AtlasEntityHeader> entityHeader;
        entityHeader = discovery.searchUsingTermQualifiedName(0,1,termQName,null,null);
        Boolean hasEntityAssociation = entityHeader != null ? true : false;
        return hasEntityAssociation;
    }

    public List<AtlasEntityHeader> indexSearchPaginated(Map<String, Object> dsl) throws AtlasBaseException {
        IndexSearchParams searchParams = new IndexSearchParams();
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<Map> sortList = new ArrayList<>(0);
        sortList.add(mapOf("__timestamp", mapOf("order", "asc")));
        sortList.add(mapOf("__guid", mapOf("order", "asc")));
        dsl.put("sort", sortList);

        int from = 0;
        int size = 1;
        boolean hasMore = true;
        do {
            dsl.put("from", from);
            dsl.put("size", size);
            searchParams.setDsl(dsl);

            List<AtlasEntityHeader> headers = discovery.directIndexSearch(searchParams).getEntities();

            if (CollectionUtils.isNotEmpty(headers)) {
                ret.addAll(headers);
            } else {
                hasMore = false;
            }

            from += size;

        } while (hasMore);

        return ret;
    }

    public void updateMeaningsAttributesInEntitiesOnTermUpdate(String currentTermName, String updatedTermName, String termQName, String updatedTermQName, String termGuid) throws AtlasBaseException {
        Set<String> attributes = new HashSet<String>(){{
            add(ATTR_MEANINGS);
        }};

        Set<String> relationAttributes = new HashSet<String>(){{
            add(STATE_PROPERTY_KEY);
            add(NAME);
        }};

        int from = 0;
        while (true) {
            List<AtlasEntityHeader> entityHeaders = discovery.searchUsingTermQualifiedName(from, ELASTICSEARCH_PAGINATION_SIZE,
                    termQName, attributes, relationAttributes);

            if (entityHeaders == null)
                break;

            for (AtlasEntityHeader entityHeader : entityHeaders) {
                AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityHeader.getGuid());

                if (!currentTermName.equals(updatedTermName)) {
                    List<AtlasObjectId> meanings = (List<AtlasObjectId>) entityHeader.getAttribute(ATTR_MEANINGS);

                    String updatedMeaningsText = meanings
                            .stream()
                            .filter(x -> AtlasEntity.Status.ACTIVE.name().equals(x.getAttributes().get(STATE_PROPERTY_KEY)))
                            .map(x -> x.getGuid().equals(termGuid) ? updatedTermName : x.getAttributes().get(NAME).toString())
                            .collect(Collectors.joining(","));

                    AtlasGraphUtilsV2.setEncodedProperty(entityVertex, MEANINGS_TEXT_PROPERTY_KEY, updatedMeaningsText);
                    List<String> meaningsNames = entityVertex.getMultiValuedProperty(MEANING_NAMES_PROPERTY_KEY, String.class);

                    if (meaningsNames.contains(currentTermName)) {
                        AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANING_NAMES_PROPERTY_KEY, currentTermName);
                        AtlasGraphUtilsV2.addListProperty(entityVertex, MEANING_NAMES_PROPERTY_KEY, updatedTermName, true);
                    }
                }

                if (StringUtils.isNotEmpty(updatedTermQName) && !termQName.equals(updatedTermQName)) {
                    AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANINGS_PROPERTY_KEY, updatedTermQName);
                    AtlasGraphUtilsV2.addListProperty(entityVertex, MEANINGS_PROPERTY_KEY, updatedTermQName, true);
                }
            }

            from += ELASTICSEARCH_PAGINATION_SIZE;

            if (entityHeaders.size() < ELASTICSEARCH_PAGINATION_SIZE) {
                break;
            }
        }
    }

    /**
     * Get all the active parents
     * @param vertex entity vertex
     * @param parentEdgeLabel Edge label of parent
     * @return Iterator of children vertices
     */
    protected Iterator<AtlasVertex> getActiveParents(AtlasVertex vertex, String parentEdgeLabel) throws AtlasBaseException {
        return getEdges(vertex, parentEdgeLabel, AtlasEdgeDirection.IN);
    }

    /**
     * Get all the active children of category
     * @param vertex entity vertex
     * @param childrenEdgeLabel Edge label of children
     * @return Iterator of children vertices
     */
    protected Iterator<AtlasVertex> getActiveChildren(AtlasVertex vertex, String childrenEdgeLabel) throws AtlasBaseException {
        return getEdges(vertex, childrenEdgeLabel, AtlasEdgeDirection.OUT);
    }

    protected Iterator getEdges(AtlasVertex vertex, String childrenEdgeLabel, AtlasEdgeDirection direction) throws AtlasBaseException {
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

}
