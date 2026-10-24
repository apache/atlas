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
package org.apache.atlas.repository.store.bootstrap;

import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.patches.AtlasPatchRegistry;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.tasks.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class TypeDefBootstrapTaskFactory implements TaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefBootstrapTaskFactory.class);

    public static final String TYPEDEF_BOOTSTRAP_LOAD = "TYPEDEF_BOOTSTRAP_LOAD";
    public static final String MODELS_FOLDER_PATH     = "modelsFolderPath";

    private static final List<String> SUPPORTED_TYPES = new ArrayList<>(Collections.singletonList(TYPEDEF_BOOTSTRAP_LOAD));

    public static AbstractTask createBootstrapTask(AtlasTask task, AtlasPatchRegistry patchRegistry, ModelsFolderLoader modelsFolderLoader) {
        return new TypeDefBootstrapTask(task, patchRegistry, modelsFolderLoader);
    }

    @Override
    public AbstractTask create(AtlasTask task) {
        LOG.warn("Type: {} - {} cannot be created without bootstrap context; use createBootstrapTask() during typedef bootstrap",
                task.getType(), task.getGuid());

        return null;
    }

    @Override
    public List<String> getSupportedTypes() {
        return SUPPORTED_TYPES;
    }
}
