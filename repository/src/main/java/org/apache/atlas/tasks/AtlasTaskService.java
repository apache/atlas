package org.apache.atlas.tasks;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchParams;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.Constants.TASK_GUID;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;
import static org.apache.atlas.tasks.TaskRegistry.toAtlasTask;

@Component
public class AtlasTaskService implements TaskService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasTaskService.class);

    private final AtlasGraph graph;

    private final List<String> retryAllowedStatuses;

    @Inject
    public AtlasTaskService(AtlasGraph graph) {
        this.graph = graph;
        retryAllowedStatuses = new ArrayList<>();
        retryAllowedStatuses.add(AtlasTask.Status.COMPLETE.toString());
        retryAllowedStatuses.add(AtlasTask.Status.FAILED.toString());
        // Since classification vertex is deleted after the task gets deleted, no need to retry the DELETED task
    }

    @Override
    public TaskSearchResult getTasks(TaskSearchParams searchParams) throws AtlasBaseException {
        TaskSearchResult ret = new TaskSearchResult();
        List<AtlasTask> tasks = new ArrayList<>();
        AtlasIndexQuery indexQuery = null;
        DirectIndexQueryResult indexQueryResult;

        try {
            indexQuery = searchTask(searchParams);
            indexQueryResult = indexQuery.vertices(searchParams);

            if (indexQueryResult != null) {
                Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

                while (iterator.hasNext()) {
                    AtlasVertex vertex = iterator.next().getVertex();

                    if (vertex != null) {
                        tasks.add(toAtlasTask(vertex));
                    } else {
                        LOG.warn("Null vertex while fetching tasks");
                    }

                }

                ret.setTasks(tasks);
                ret.setApproximateCount(indexQuery.vertexTotals());
                ret.setAggregations(indexQueryResult.getAggregationMap());
            }
        } catch (AtlasBaseException e) {
            LOG.error("Failed to fetch tasks: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, e);
        }

        return ret;
    }

    @Override
    public TaskSearchResult getTasksByCondition(int from, int size, List<Map<String,Object>> mustConditions, List<Map<String,Object>> shouldConditions,
                                                List<Map<String,Object>> mustNotConditions) throws AtlasBaseException {
        Map<String, Object> dsl = getMap("from", from);
        dsl.put("size", size);
        Map<String, Map<String, Object>> boolCondition = Collections.singletonMap("bool", new HashMap<>());
        boolCondition.get("bool").put("must", mustConditions);
        boolCondition.get("bool").put("must_not", mustNotConditions);
        boolCondition.get("bool").put("should", shouldConditions);
        dsl.put("query", boolCondition);
        TaskSearchParams taskSearchParams = new TaskSearchParams();
        taskSearchParams.setDsl(dsl);
        TaskSearchResult tasks = getTasks(taskSearchParams);
        return tasks;
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
    @Override
    public void retryTask(String taskGuid) throws AtlasBaseException {
        TaskSearchParams taskSearchParams = getMatchQuery(taskGuid);
        AtlasIndexQuery atlasIndexQuery = searchTask(taskSearchParams);
        DirectIndexQueryResult indexQueryResult = atlasIndexQuery.vertices(taskSearchParams);

        AtlasVertex atlasVertex = getTaskVertex(indexQueryResult.getIterator(), taskGuid);

        String status = atlasVertex.getProperty(Constants.TASK_STATUS, String.class);

        // Retrial ability of the task is not limited to FAILED ones due to testing/debugging
        if (! retryAllowedStatuses.contains(status)) {
            throw new AtlasBaseException(AtlasErrorCode.TASK_STATUS_NOT_APPROPRIATE, taskGuid, status);
        }

        setEncodedProperty(atlasVertex, Constants.TASK_STATUS, AtlasTask.Status.PENDING);
        int attemptCount = atlasVertex.getProperty(Constants.TASK_ATTEMPT_COUNT, Integer.class);
        setEncodedProperty(atlasVertex, Constants.TASK_ATTEMPT_COUNT, attemptCount+1);
        graph.commit();
    }

    private AtlasVertex getTaskVertex(Iterator<AtlasIndexQuery.Result> iterator, String taskGuid) throws AtlasBaseException {
        while(iterator.hasNext()) {
            AtlasVertex atlasVertex = iterator.next().getVertex();
            if (atlasVertex.getProperty(Constants.TASK_GUID, String.class).equals(taskGuid)) {
                return atlasVertex;
            }
        }
        throw new AtlasBaseException(AtlasErrorCode.TASK_NOT_FOUND, taskGuid);
    }

    private TaskSearchParams getMatchQuery(String guid) {
        TaskSearchParams params = new TaskSearchParams();
        params.setDsl(mapOf("query", mapOf("match", mapOf(TASK_GUID, guid))));
        return params;
    }

    private Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }

    private AtlasIndexQuery searchTask(TaskSearchParams searchParams) {
        return graph.elasticsearchQuery(Constants.VERTEX_INDEX, searchParams);
    }
}
