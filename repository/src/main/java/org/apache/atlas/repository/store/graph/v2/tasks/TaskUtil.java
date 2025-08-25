package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.tasks.AtlasTaskService;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;

public class TaskUtil {
    final public        AtlasTaskService    taskService;
    final public        AtlasGraph          graph;
    final public static String              TASK_STATUS_PENDING = "PENDING";

    public TaskUtil(AtlasGraph graph) {
        this.graph = graph;
        taskService = new AtlasTaskService(graph);
    }

    public TaskSearchResult findPendingTasksByClassificationId(int from, int size, String classificationId, List<String> types, List<String> excludeTypes) throws AtlasBaseException {
        List<Map<String,Object>> mustConditions = new ArrayList<>();
        List<Map<String,Object>> shouldConditions = new ArrayList<>();
        List<Map<String,Object>> excludeConditions = new ArrayList<>();

        if (StringUtils.isNotEmpty(classificationId))
            mustConditions.add(getMap("term", getMap(TASK_CLASSIFICATION_ID, classificationId)));

        if (CollectionUtils.isNotEmpty(types)) {
            List<Map<String, Object>> typeQueries = types.stream().map(type -> getMap("match", getMap(TASK_TYPE, type))).collect(Collectors.toList());
            shouldConditions.addAll(typeQueries);
        }

        if(CollectionUtils.isNotEmpty(excludeConditions)) {
            List<Map<String, Object>> excludeTypeQueries = excludeTypes.stream().map(type -> getMap("match", getMap(TASK_TYPE, type))).collect(Collectors.toList());
            excludeConditions.addAll(excludeTypeQueries);
        }

        Map<String, Object> statusQuery = getMap("match", getMap(TASK_STATUS, TASK_STATUS_PENDING));
        mustConditions.add(statusQuery);

        return taskService.getTasksByCondition(from, size, mustConditions, shouldConditions, excludeConditions);

    }

    /**
     *
     * Finds a single page of pending tasks that match the specified criteria, starting from a given offset.
     *
     * @param from        The starting offset for pagination.
     * @param size        The page size.
     * @param entityGuid  The GUID of the entity.
     * @param tagTypeName The type name of the classification/tag.
     * @param types       A list of task types to search for.
     * @return A list of tasks from the specified page of results.
     * @throws AtlasBaseException
     */
    public List<AtlasTask> findAPageOfPendingTasks(int from, int size, String entityGuid, String tagTypeName, List<String> types) throws AtlasBaseException {
        List<Map<String,Object>> mustConditions = new ArrayList<>();

        if (StringUtils.isNotEmpty(entityGuid))
            mustConditions.add(getMap("term", getMap(TASK_ENTITY_GUID, entityGuid)));

        if (StringUtils.isNotEmpty(tagTypeName))
            mustConditions.add(getMap("term", getMap(TASK_CLASSIFICATION_TYPENAME, tagTypeName)));

        if (CollectionUtils.isNotEmpty(types)) {
            mustConditions.add(getMap("terms", getMap(TASK_TYPE, types)));
        }

        // The ES query is filtered by PENDING status, though results must be validated due to potential sync issues/delays.
        mustConditions.add(getMap("term", getMap(TASK_STATUS + ".keyword", TASK_STATUS_PENDING)));

        return taskService.getTasksByCondition(from, size, mustConditions);
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }



}
