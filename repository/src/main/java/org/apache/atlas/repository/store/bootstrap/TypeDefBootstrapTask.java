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

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.patches.AtlasPatchRegistry;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.store.bootstrap.TypeDefBootstrapTaskFactory.MODELS_FOLDER_PATH;

public class TypeDefBootstrapTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefBootstrapTask.class);

    private final AtlasPatchRegistry patchRegistry;
    private final ModelsFolderLoader modelsFolderLoader;

    public TypeDefBootstrapTask(AtlasTask task, AtlasPatchRegistry patchRegistry, ModelsFolderLoader modelsFolderLoader) {
        super(task);

        this.patchRegistry       = patchRegistry;
        this.modelsFolderLoader  = modelsFolderLoader;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        RequestContext.clear();

        Map<String, Object> params = getTaskDef().getParameters();

        if (params == null || !params.containsKey(MODELS_FOLDER_PATH)) {
            LOG.warn("Task: {}: Missing models folder path parameter", getTaskGuid());

            return FAILED;
        }

        Object folderPathObj = params.get(MODELS_FOLDER_PATH);

        if (!(folderPathObj instanceof String) || StringUtils.isEmpty((String) folderPathObj)) {
            LOG.warn("Task: {}: Invalid models folder path parameter", getTaskGuid());

            return FAILED;
        }

        File modelsFolder = new File((String) folderPathObj);

        if (!modelsFolder.isDirectory()) {
            LOG.warn("Task: {}: Models folder does not exist or is not a directory: {}", getTaskGuid(), modelsFolder.getAbsolutePath());

            return FAILED;
        }

        if (patchRegistry == null || modelsFolderLoader == null) {
            LOG.warn("Task: {}: Bootstrap loader context is not available", getTaskGuid());

            return FAILED;
        }

        try {
            modelsFolderLoader.load(modelsFolder, patchRegistry);

            setStatus(COMPLETE);
        } catch (Exception e) {
            LOG.error("Task: {}: Failed to load typedef models from folder {}", getTaskGuid(), modelsFolder.getAbsolutePath(), e);

            setStatus(FAILED);

            throw e;
        } finally {
            RequestContext.clear();
        }

        return getStatus();
    }
}
