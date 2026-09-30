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
package org.apache.atlas.glossary;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.GlossaryExportParameters;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.atlas.model.tasks.AtlasTask.Status.COMPLETE;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;

/**
 * Background task that writes a glossary export file (CSV or XLSX) to the user's download directory.
 * Mirrors {@link org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask}.
 */
public class GlossaryExportDownloadTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryExportDownloadTask.class);

    public static final String EXPORT_PARAMETERS_JSON_KEY = "export_parameters_json";
    public static final String FILE_NAME_KEY              = "file_name";

    private static final String DOWNLOAD_DIR_PATH_KEY     = "atlas.download.glossary.dir.path";
    private static final String DOWNLOAD_DIR_PATH_DEFAULT = System.getProperty("user.dir");
    private static final String GLOSSARY_DOWNLOAD_DIR     = "glossary_export_downloads";

    public static final String DOWNLOAD_DIR_PATH;

    private final GlossaryService glossaryService;

    public GlossaryExportDownloadTask(AtlasTask task, GlossaryService glossaryService) {
        super(task);
        this.glossaryService = glossaryService;
    }

    @Override
    public AtlasTask.Status perform() throws Exception {
        RequestContext.clear();

        Map<String, Object> params = getTaskDef().getParameters();

        if (MapUtils.isEmpty(params)) {
            LOG.warn("Task: {}: Unable to process task: Parameters is not readable!", getTaskGuid());
            return FAILED;
        }

        String userName = getTaskDef().getCreatedBy();

        if (StringUtils.isEmpty(userName)) {
            LOG.warn("Task: {}: Unable to process task as user name is empty!", getTaskGuid());
            return FAILED;
        }

        RequestContext.get().setUser(userName, null);

        try {
            run(params);
            setStatus(COMPLETE);
        } catch (Exception e) {
            LOG.error("Task: {}: Error performing glossary export download task!", getTaskGuid(), e);
            setStatus(FAILED);
            throw e;
        } finally {
            RequestContext.clear();
        }

        return getStatus();
    }

    protected void run(Map<String, Object> parameters) throws AtlasBaseException, IOException {
        String exportParametersJson = (String) parameters.get(EXPORT_PARAMETERS_JSON_KEY);
        String fileName             = (String) parameters.get(FILE_NAME_KEY);

        GlossaryExportParameters exportParameters = AtlasJson.fromJson(exportParametersJson, GlossaryExportParameters.class);

        if (exportParameters == null || exportParameters.getFormat() == null) {
            throw new AtlasBaseException(org.apache.atlas.AtlasErrorCode.BAD_REQUEST, "Export format (CSV or XLSX) is required");
        }

        File dir = new File(DOWNLOAD_DIR_PATH, RequestContext.getCurrentUser());

        if (!dir.exists()) {
            dir.mkdirs();
        }

        File exportFile = new File(dir, fileName);

        try (OutputStream outputStream = new FileOutputStream(exportFile)) {
            glossaryService.exportGlossary(exportParameters, outputStream);
        }

        LOG.info("Glossary export file generated: {}", exportFile.getAbsolutePath());
    }

    static {
        Configuration configuration = null;

        try {
            configuration = ApplicationProperties.get();
        } catch (AtlasException e) {
            LOG.error("Error fetching application properties", e);
        }

        if (configuration != null) {
            DOWNLOAD_DIR_PATH = configuration.getString(DOWNLOAD_DIR_PATH_KEY, DOWNLOAD_DIR_PATH_DEFAULT)
                    + File.separator + GLOSSARY_DOWNLOAD_DIR;
        } else {
            DOWNLOAD_DIR_PATH = DOWNLOAD_DIR_PATH_DEFAULT + File.separator + GLOSSARY_DOWNLOAD_DIR;
        }
    }
}
