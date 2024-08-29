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
package org.apache.atlas.tasks;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchParams;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.DirectIndexQueryResult;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.TASK_GUID;
import static org.apache.atlas.repository.Constants.TASK_STATUS;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.setEncodedProperty;

@Component
public class TaskRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRegistry.class);

    private AtlasGraph graph;
    private TaskService taskService;
    private int queueSize;
    private boolean useGraphQuery;

    @Inject
    public TaskRegistry(AtlasGraph graph, TaskService taskService) {
        this.graph = graph;
        this.taskService = taskService;
        queueSize = AtlasConfiguration.TASKS_QUEUE_SIZE.getInt();
        useGraphQuery = AtlasConfiguration.TASKS_REQUEUE_GRAPH_QUERY.getBoolean();
    }

    @GraphTransaction
    public AtlasTask save(AtlasTask task) {
        AtlasVertex vertex = createVertex(task);

        return toAtlasTask(vertex);
    }

    public List<AtlasTask> getPendingTasks() {
        List<AtlasTask> ret = new ArrayList<>();

        try {
            AtlasGraphQuery query = graph.query()
                                         .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                                         .has(Constants.TASK_STATUS, AtlasTask.Status.PENDING)
                                         .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

            Iterator<AtlasVertex> results = query.vertices().iterator();

            while (results.hasNext()) {
                AtlasVertex vertex = results.next();

                if(vertex != null) {
                    ret.add(toAtlasTask(vertex));
                }
            }
        } catch (Exception exception) {
            LOG.error("Error fetching pending tasks!", exception);
        }

        return ret;
    }

    public List<AtlasTask> getInProgressTasks() {
        List<AtlasTask> ret = new ArrayList<>();

        try {
            AtlasGraphQuery query = graph.query()
                    .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                    .has(Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS)
                    .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

            Iterator<AtlasVertex> results = query.vertices().iterator();

            while (results.hasNext()) {
                AtlasVertex vertex = results.next();

                if(vertex != null) {
                    ret.add(toAtlasTask(vertex));
                }
            }
        } catch (Exception exception) {
            LOG.error("Error fetching in progress tasks!", exception);
        }

        return ret;
    }

    @GraphTransaction
    public void updateStatus(AtlasVertex taskVertex, AtlasTask task) {
        if (taskVertex == null) {
            return;
        }

        setEncodedProperty(taskVertex, Constants.TASK_ATTEMPT_COUNT, task.getAttemptCount());
        setEncodedProperty(taskVertex, Constants.TASK_STATUS, task.getStatus().toString());
        setEncodedProperty(taskVertex, Constants.TASK_UPDATED_TIME, System.currentTimeMillis());
        setEncodedProperty(taskVertex, Constants.TASK_ERROR_MESSAGE, task.getErrorMessage());
    }

    @GraphTransaction
    public void deleteByGuid(String guid) throws AtlasBaseException {
        try {
            taskService.hardDelete(guid);
        } catch (Exception exception) {
            LOG.error("Error: deletingByGuid: {}", guid);

            throw new AtlasBaseException(exception);
        } finally {
            graph.commit();
        }
    }

    @GraphTransaction
    public void softDelete(String guid) throws AtlasBaseException{
        try {
            taskService.softDelete(guid);
        }
        catch (Exception exception) {
            LOG.error("Error: on soft delete: {}", guid);

            throw new AtlasBaseException(exception);
        }
    }

    public List<AtlasTask> getByIdsES(List<String> guids) throws AtlasBaseException {
        List<AtlasTask> ret = new ArrayList<>();

        List<List<String>> chunkedGuids = ListUtils.partition(guids, 50);

        for (List<String> chunkedGuidList : chunkedGuids) {
            List<Map> should = new ArrayList<>();
            for (String guid : chunkedGuidList) {
                should.add(mapOf("match", mapOf(TASK_GUID, guid)));
            }

            TaskSearchParams params = new TaskSearchParams();
            params.setDsl(mapOf("query", mapOf("bool", mapOf("should", should))));

            TaskSearchResult result = taskService.getTasks(params);
            if (result == null) {
                return null;
            }

            // as __task_guid is text field, might result multiple results due to "-" tokenizing in ES
            // adding filtering layer to filter exact tasks
            ret.addAll(filterTasksByGuids(result.getTasks(), chunkedGuidList));
        }

        return ret;
    }

    private List<AtlasTask> filterTasksByGuids(List<AtlasTask> tasks, List<String> guidList) {
        return tasks.stream().filter(task -> guidList.contains(task.getGuid())).collect(Collectors.toList());
    }

    @GraphTransaction
    public void deleteComplete(AtlasVertex taskVertex, AtlasTask task) {
        updateStatus(taskVertex, task);

        deleteVertex(taskVertex);
    }

    public void inProgress(AtlasVertex taskVertex, AtlasTask task) {
        RequestContext.get().setCurrentTask(task);

        task.setStartTime(new Date());

        setEncodedProperty(taskVertex, Constants.TASK_START_TIME, task.getStartTime());
        setEncodedProperty(taskVertex, Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS);
        setEncodedProperty(taskVertex, Constants.TASK_UPDATED_TIME, System.currentTimeMillis());
        graph.commit();
    }

    @GraphTransaction
    public void complete(AtlasVertex taskVertex, AtlasTask task) {
        if (task.getEndTime() != null) {
            setEncodedProperty(taskVertex, Constants.TASK_END_TIME, task.getEndTime());

            if (task.getStartTime() == null) {
                LOG.warn("Task start time was not recorded since could not calculate task's total take taken");
            } else {
                long timeTaken = task.getEndTime().getTime() - task.getStartTime().getTime();
                timeTaken = TimeUnit.MILLISECONDS.toSeconds(timeTaken);
                setEncodedProperty(taskVertex, Constants.TASK_TIME_TAKEN_IN_SECONDS, timeTaken);
            }
        }

        updateStatus(taskVertex, task);

        LOG.info(String.format("TaskRegistry complete %s", task.toString()));
    }

    @GraphTransaction
    public AtlasTask getById(String guid) {
        AtlasGraphQuery query = graph.query()
                                     .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                                     .has(TASK_GUID, guid);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        return results.hasNext() ? toAtlasTask(results.next()) : null;
    }

    @GraphTransaction
    public AtlasVertex getVertex(String taskGuid) {
        AtlasGraphQuery query = graph.query().has(Constants.TASK_GUID, taskGuid);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        return results.hasNext() ? results.next() : null;
    }

    @GraphTransaction
    public List<AtlasTask> getAll() {
        List<AtlasTask> ret = new ArrayList<>();
        AtlasGraphQuery query = graph.query()
                                     .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME)
                                     .orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

        Iterator<AtlasVertex> results = query.vertices().iterator();

        while (results.hasNext()) {
            ret.add(toAtlasTask(results.next()));
        }

        return ret;
    }

    /*
    * This returns tasks which has status IN statusList
    * If not specified, return all tasks
    * */
    @GraphTransaction
    public List<AtlasTask> getAll(List<String> statusList, int offset, int limit) {
        List<AtlasTask> ret = new ArrayList<>();
        AtlasGraphQuery query = graph.query()
                                     .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME);

        if (CollectionUtils.isNotEmpty(statusList)) {
            List<AtlasGraphQuery> orConditions = new LinkedList<>();

            for (String status : statusList) {
                orConditions.add(query.createChildQuery().has(Constants.TASK_STATUS, AtlasTask.Status.from(status)));
            }

            query.or(orConditions);
        }

        query.orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.DESC);

        Iterator<AtlasVertex> results = query.vertices(offset, limit).iterator();

        while (results.hasNext()) {
            ret.add(toAtlasTask(results.next()));
        }

        return ret;
    }

    public List<AtlasTask> getTasksForReQueue() {
        List<AtlasTask> ret = null;

        if (useGraphQuery) {
            ret = getTasksForReQueueGraphQuery();
        } else {
            ret = getTasksForReQueueIndexSearch();
        }

        return ret;
    }

    public List<AtlasTask> getTasksForReQueueGraphQuery() {

        List<AtlasTask> ret = new ArrayList<>();
        AtlasGraphQuery query = graph.query()
                .has(Constants.TASK_TYPE_PROPERTY_KEY, Constants.TASK_TYPE_NAME);

        List<AtlasGraphQuery> orConditions = new LinkedList<>();
        orConditions.add(query.createChildQuery().has(Constants.TASK_STATUS, AtlasTask.Status.IN_PROGRESS));
        orConditions.add(query.createChildQuery().has(Constants.TASK_STATUS, AtlasTask.Status.PENDING));
        query.or(orConditions);

        query.orderBy(Constants.TASK_CREATED_TIME, AtlasGraphQuery.SortOrder.ASC);

        Iterator<AtlasVertex> results = query.vertices(queueSize).iterator();

        while (results.hasNext()) {
            AtlasVertex vertex = results.next();

            if (vertex != null) {
                ret.add(toAtlasTask(vertex));
            } else {
                LOG.error("Null vertex while re-queuing tasks");
            }
        }

        return ret;
    }

    public List<AtlasTask> getTasksForReQueueIndexSearch() {
        DirectIndexQueryResult indexQueryResult = null;
        List<AtlasTask> ret = new ArrayList<>();

        int size = 1000;
        int from = 0;

        IndexSearchParams indexSearchParams = new IndexSearchParams();

        List statusClauseList = new ArrayList();
        statusClauseList.add(mapOf("match", mapOf(TASK_STATUS, AtlasTask.Status.IN_PROGRESS.toString())));
        statusClauseList.add(mapOf("match", mapOf(TASK_STATUS, AtlasTask.Status.PENDING.toString())));

        Map<String, Object> dsl = mapOf("query", mapOf("bool", mapOf("should", statusClauseList)));
        dsl.put("sort", Collections.singletonList(mapOf(Constants.TASK_CREATED_TIME, mapOf("order", "asc"))));
        dsl.put("size", size);
        int totalFetched = 0;
        while (true) {
            int fetched = 0;
            try {
                if (totalFetched + size > queueSize) {
                    size = queueSize - totalFetched;
                }

                dsl.put("from", from);
                dsl.put("size", size);

                indexSearchParams.setDsl(dsl);

                AtlasIndexQuery indexQuery = graph.elasticsearchQuery(Constants.VERTEX_INDEX, indexSearchParams);

                try {
                    indexQueryResult = indexQuery.vertices(indexSearchParams);
                } catch (AtlasBaseException e) {
                    LOG.error("Failed to fetch pending/in-progress task vertices to re-que");
                    e.printStackTrace();
                    break;
                }

                if (indexQueryResult != null) {
                    Iterator<AtlasIndexQuery.Result> iterator = indexQueryResult.getIterator();

                    while (iterator.hasNext()) {
                        AtlasVertex vertex = iterator.next().getVertex();

                        if (vertex != null) {
                            AtlasTask atlasTask = toAtlasTask(vertex);
                            if (atlasTask.getStatus().equals(AtlasTask.Status.PENDING) ||
                                    atlasTask.getStatus().equals(AtlasTask.Status.IN_PROGRESS) ){
                                LOG.info(String.format("Fetched task from index search: %s", atlasTask.toString()));
                                ret.add(atlasTask);
                            }
                            else {
                                LOG.warn(String.format("There is a mismatch on tasks status between ES and Cassandra for guid: %s", atlasTask.getGuid()));
                            }
                        } else {
                            LOG.warn("Null vertex while re-queuing tasks at index {}", fetched);
                        }

                        fetched++;
                    }
                }

                totalFetched += fetched;
                from += size;
                if (fetched < size || totalFetched >= queueSize) {
                    break;
                }
            } catch (Exception e){
                break;
            }
        }

        return ret;
    }

    public void commit() {
        this.graph.commit();
    }

    public AtlasTask createVertex(String taskType, String createdBy, Map<String, Object> parameters, String classificationId, String entityGuid) {
        AtlasTask ret = new AtlasTask(taskType, createdBy, parameters, classificationId, entityGuid);

        createVertex(ret);

        return ret;
    }

    public AtlasTask createVertex(String taskType, String createdBy, Map<String, Object> parameters, String classificationId,String classificationName, String entityGuid) {
        AtlasTask ret = new AtlasTask(taskType, createdBy, parameters, classificationId, entityGuid);
        ret.setClassificationName(classificationName);
        createVertex(ret);

        return ret;
    }

    private void deleteVertex(AtlasVertex taskVertex) {
        if (taskVertex == null) {
            return;
        }

        graph.removeVertex(taskVertex);
    }

    public static AtlasTask toAtlasTask(AtlasVertex v) {
        AtlasTask ret = new AtlasTask();

        String guid = v.getProperty(Constants.TASK_GUID, String.class);
        if (guid != null) {
            ret.setGuid(guid);
        }

        String type = v.getProperty(Constants.TASK_TYPE, String.class);
        if (type != null) {
            ret.setType(type);
        }

        String status = v.getProperty(Constants.TASK_STATUS, String.class);
        if (status != null) {
            ret.setStatus(status);
        }

        String createdBy = v.getProperty(Constants.TASK_CREATED_BY, String.class);
        if (createdBy != null) {
            ret.setCreatedBy(createdBy);
        }

        Long createdTime = v.getProperty(Constants.TASK_CREATED_TIME, Long.class);
        if (createdTime != null) {
            ret.setCreatedTime(new Date(createdTime));
        }

        Long updatedTime = v.getProperty(Constants.TASK_UPDATED_TIME, Long.class);
        if (updatedTime != null) {
            ret.setUpdatedTime(new Date(updatedTime));
        }

        Long startTime = v.getProperty(Constants.TASK_START_TIME, Long.class);
        if (startTime != null) {
            ret.setStartTime(new Date(startTime));
        }

        Long endTime = v.getProperty(Constants.TASK_END_TIME, Long.class);
        if (endTime != null) {
            ret.setEndTime(new Date(endTime));

            Long timeTaken = v.getProperty(Constants.TASK_TIME_TAKEN_IN_SECONDS, Long.class);
            if (timeTaken != null) {
                ret.setTimeTakenInSeconds(timeTaken);
            }
        }

        String parametersJson = v.getProperty(Constants.TASK_PARAMETERS, String.class);
        if (parametersJson != null) {
            ret.setParameters(AtlasType.fromJson(parametersJson, Map.class));
        }

        String classificationId = v.getProperty(Constants.TASK_CLASSIFICATION_ID, String.class);
        if (classificationId != null) {
            ret.setClassificationId(classificationId);
        }

        String classificationName = v.getProperty(Constants.TASK_CLASSIFICATION_TYPENAME, String.class);
        if (classificationName != null) {
            ret.setClassificationName(classificationName);
        }

        String entityGuid = v.getProperty(Constants.TASK_ENTITY_GUID, String.class);
        if(entityGuid != null) {
            ret.setEntityGuid(entityGuid);
        }

        Integer attemptCount = v.getProperty(Constants.TASK_ATTEMPT_COUNT, Integer.class);
        if (attemptCount != null) {
            ret.setAttemptCount(attemptCount);
        }

        String errorMessage = v.getProperty(Constants.TASK_ERROR_MESSAGE, String.class);
        if (errorMessage != null) {
            ret.setErrorMessage(errorMessage);
        }


        return ret;
    }

    private AtlasVertex createVertex(AtlasTask task) {
        return taskService.createTaskVertex(task);
    }

    private Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }
}