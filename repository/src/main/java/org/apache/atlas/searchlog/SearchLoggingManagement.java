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
package org.apache.atlas.searchlog;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.model.searchlog.SearchRequestLogData;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class SearchLoggingManagement {
    private static final Logger LOG = LoggerFactory.getLogger(SearchLoggingManagement.class);

    private final List<SearchLogger> esSearchLoggers;
    private final ExecutorService executorService;

    @Inject
    public SearchLoggingManagement(List<SearchLogger> esSearchLoggers) {
        this.esSearchLoggers        = esSearchLoggers;

        this.executorService = Executors.newFixedThreadPool(AtlasConfiguration.SEARCH_LOGGER_MAX_THREADS.getInt(),
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("atlas-search-logger-%d")
                    .build()
        );
    }

    public void log(SearchRequestLogData searchRequestLogData) {
        SearchLoggingConsumer loggerConsumer = new SearchLoggingConsumer(esSearchLoggers, searchRequestLogData);
        this.executorService.submit(loggerConsumer);
    }
}