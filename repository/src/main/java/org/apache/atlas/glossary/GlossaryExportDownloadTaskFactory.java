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
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.tasks.AbstractTask;
import org.apache.atlas.tasks.TaskFactory;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@Singleton
@Service
@EnableScheduling
public class GlossaryExportDownloadTaskFactory implements TaskFactory {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryExportDownloadTaskFactory.class);

    public static final  String GLOSSARY_EXPORT_DOWNLOAD              = "GLOSSARY_EXPORT_DOWNLOAD";

    private static final int    MAX_PENDING_TASKS_ALLOWED_DEFAULT     = 50;
    private static final String MAX_PENDING_TASKS_ALLOWED_KEY         = "atlas.download.glossary.max.pending.tasks";
    private static final String FILES_CLEANUP_INTERVAL                = "0 0/1 * * * *";
    private static final long   FILE_EXP_DURATION_IN_MILLIS_DEFAULT   = 24 * 60 * 60 * 1000;
    private static final String FILE_EXP_DURATION_IN_MILLIS_KEY         = "atlas.download.glossary.file.expiry.millis";
    private static final List<String> SUPPORTED_TYPES                 = new ArrayList<>(Collections.singletonList(GLOSSARY_EXPORT_DOWNLOAD));

    public  static final int  MAX_PENDING_TASKS_ALLOWED;
    private static final long FILE_EXP_DURATION_IN_MILLIS;

    private final GlossaryService glossaryService;

    @Inject
    public GlossaryExportDownloadTaskFactory(GlossaryService glossaryService) {
        this.glossaryService = glossaryService;
    }

    @Override
    public AbstractTask create(AtlasTask task) {
        String taskType = task.getType();
        String taskGuid = task.getGuid();

        switch (taskType) {
            case GLOSSARY_EXPORT_DOWNLOAD:
                return new GlossaryExportDownloadTask(task, glossaryService);

            default:
                LOG.warn("Type: {} - {} not found!. The task will be ignored.", taskType, taskGuid);
                return null;
        }
    }

    @Override
    public List<String> getSupportedTypes() {
        return SUPPORTED_TYPES;
    }

    @Scheduled(cron = "#{getGlossaryExportCronExpressionForCleanup}")
    public void cleanupExpiredFiles() {
        File downloadDir = new File(GlossaryExportDownloadTask.DOWNLOAD_DIR_PATH);

        deleteFiles(downloadDir);
    }

    @Bean
    private String getGlossaryExportCronExpressionForCleanup() {
        return FILES_CLEANUP_INTERVAL;
    }

    private void deleteFiles(File downloadDir) {
        File[] subDirs = downloadDir.listFiles();

        if (ArrayUtils.isNotEmpty(subDirs)) {
            for (File subDir : subDirs) {
                File[] exportFiles = subDir.listFiles();

                if (ArrayUtils.isNotEmpty(exportFiles)) {
                    for (File exportFile : exportFiles) {
                        BasicFileAttributes attr;

                        try {
                            attr = Files.readAttributes(exportFile.toPath(), BasicFileAttributes.class);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        Date createdTime = new Date(attr.creationTime().toMillis());

                        if (System.currentTimeMillis() - createdTime.getTime() > FILE_EXP_DURATION_IN_MILLIS) {
                            if (!exportFile.delete()) {
                                LOG.warn("Failed to delete expired glossary export file: {}", exportFile.getAbsolutePath());
                            }
                        }
                    }
                }
            }
        }
    }

    static {
        Configuration configuration = null;

        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.error("Error fetching application properties", e);
        }

        if (configuration != null) {
            MAX_PENDING_TASKS_ALLOWED   = configuration.getInt(MAX_PENDING_TASKS_ALLOWED_KEY, MAX_PENDING_TASKS_ALLOWED_DEFAULT);
            FILE_EXP_DURATION_IN_MILLIS = configuration.getLong(FILE_EXP_DURATION_IN_MILLIS_KEY, FILE_EXP_DURATION_IN_MILLIS_DEFAULT);
        } else {
            MAX_PENDING_TASKS_ALLOWED   = MAX_PENDING_TASKS_ALLOWED_DEFAULT;
            FILE_EXP_DURATION_IN_MILLIS = FILE_EXP_DURATION_IN_MILLIS_DEFAULT;
        }
    }
}
