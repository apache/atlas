/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.tasks;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchParams;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.List;
import java.util.Map;

public interface TaskService {

    /**
     * Search for direct ES query to fetch Tasks
     * @param searchParams Search criteria
     * @return Matching tasks
     * @throws AtlasBaseException
     */
    TaskSearchResult getTasks(TaskSearchParams searchParams) throws AtlasBaseException;

    /**
     *
     * @param from
     * @param size
     * @param mustConditions
     * @param shouldConditions
     * @param mustNotConditions
     * @return
     * @throws AtlasBaseException
     */
    TaskSearchResult getTasksByCondition(int from, int size, List<Map<String,Object>> mustConditions, List<Map<String,Object>> shouldConditions,
                                         List<Map<String,Object>> mustNotConditions) throws AtlasBaseException;


    List<AtlasTask> getTasksByCondition(int from, int size, List<Map<String, Object>> mustConditions) throws AtlasBaseException;

    /**
     * Retry the task by changing its status to PENDING and increment attempt count
     * @param taskGuid Guid of the task
     * @throws AtlasBaseException
     */
    void retryTask(String taskGuid) throws AtlasBaseException;

    /**
     * Create a new task
     * @param tasks to create
     * @return Created task
     * @throws AtlasBaseException
     */
    List<AtlasTask> createAtlasTasks(List<AtlasTask> tasks) throws AtlasBaseException;

    List<AtlasTask> deleteAtlasTasks(List<AtlasTask> tasks);

    /**
     * Update a task
     * @param task to create
     * @return created task
     * @throws AtlasBaseException
     */
    AtlasVertex createTaskVertex(AtlasTask task);

    /**
     * Delete a task
     * @param guid task to delete
     * @throws AtlasBaseException
     */
    void hardDelete(String guid) throws AtlasBaseException;

    /**
     * Delete a task
     * @param guid task to soft delete
     * @throws AtlasBaseException
     */
    void softDelete(String guid) throws AtlasBaseException;

}
