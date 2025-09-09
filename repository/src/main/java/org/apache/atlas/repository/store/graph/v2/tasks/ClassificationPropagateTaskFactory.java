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
package org.apache.atlas.repository.store.graph.v2.tasks;

import org.apache.atlas.repository.metrics.TaskMetricsService;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.tasks.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@Component
public class ClassificationPropagateTaskFactory implements TaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationPropagateTaskFactory.class);

    public static final String CLASSIFICATION_PROPAGATION_TEXT_UPDATE                 = "CLASSIFICATION_PROPAGATION_TEXT_UPDATE";
    public static final String CLASSIFICATION_PROPAGATION_ADD                 = "CLASSIFICATION_PROPAGATION_ADD";

    //This should be used when referencing vertex to which classification is directly attached
    public static final String CLASSIFICATION_PROPAGATION_DELETE              = "CLASSIFICATION_PROPAGATION_DELETE";

    public static final String CLASSIFICATION_REFRESH_PROPAGATION = "CLASSIFICATION_REFRESH_PROPAGATION";

    public static final String CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE = "CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE";

    public static final String CLEANUP_CLASSIFICATION_PROPAGATION = "CLEANUP_CLASSIFICATION_PROPAGATION";



    public static final List<String> supportedTypes = new ArrayList<String>() {{
        add(CLASSIFICATION_PROPAGATION_TEXT_UPDATE);
        add(CLASSIFICATION_PROPAGATION_ADD);
        add(CLASSIFICATION_PROPAGATION_DELETE);
        add(CLASSIFICATION_REFRESH_PROPAGATION);
        add(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE);
        add(CLEANUP_CLASSIFICATION_PROPAGATION);

    }};

    private final AtlasGraph             graph;
    private final EntityGraphMapper      entityGraphMapper;
    private final DeleteHandlerDelegate  deleteDelegate;
    private final AtlasRelationshipStore relationshipStore;
    private final TaskMetricsService taskMetricsService;

    @Inject
    public ClassificationPropagateTaskFactory(AtlasGraph graph, EntityGraphMapper entityGraphMapper, DeleteHandlerDelegate deleteDelegate, AtlasRelationshipStore relationshipStore, TaskMetricsService taskMetricsService) {
        this.graph             = graph;
        this.entityGraphMapper = entityGraphMapper;
        this.deleteDelegate    = deleteDelegate;
        this.relationshipStore = relationshipStore;
        this.taskMetricsService = taskMetricsService;
    }

    public org.apache.atlas.tasks.AbstractTask create(AtlasTask task) {
        String taskType = task.getType();
        String taskGuid = task.getGuid();

        switch (taskType) {
            case CLASSIFICATION_PROPAGATION_ADD:
                return new ClassificationPropagationTasks.Add(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            case CLASSIFICATION_PROPAGATION_TEXT_UPDATE:
                return new ClassificationPropagationTasks.UpdateText(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            case CLASSIFICATION_PROPAGATION_DELETE:
                return new ClassificationPropagationTasks.Delete(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            case CLASSIFICATION_REFRESH_PROPAGATION:
                return new ClassificationPropagationTasks.RefreshPropagation(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            case CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE:
                return new ClassificationPropagationTasks.UpdateRelationship(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            case CLEANUP_CLASSIFICATION_PROPAGATION:
                return new ClassificationPropagationTasks.CleanUpClassificationPropagation(task, graph, entityGraphMapper, deleteDelegate, relationshipStore, taskMetricsService);

            default:
                LOG.warn("Type: {} - {} not found!. The task will be ignored.", taskType, taskGuid);
                return null;
        }
    }

    @Override
    public List<String> getSupportedTypes() {
        return this.supportedTypes;
    }
}
