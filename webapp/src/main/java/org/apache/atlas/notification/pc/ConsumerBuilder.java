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
package org.apache.atlas.notification.pc;

import org.apache.atlas.notification.EntityCorrelationManager;
import org.apache.atlas.notification.SerialEntityProcessor;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.impexp.AsyncImporter;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.springframework.security.core.Authentication;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ConsumerBuilder implements WorkItemBuilder<Consumer, Ticket> {
    private final AtlasEntityStore            entityStore;
    private final AtlasInstanceConverter      instanceConverter;
    private final EntityCorrelationManager    entityCorrelationManager;
    private final AtlasTypeRegistry           typeRegistry;
    private final Logger                      failedLog;
    private final Logger                      largeMessagesLog;
    private final Configuration               applicationProperties;
    private final AtlasMetricsUtil            metricsUtil;
    private final Map<String, Authentication> authnCache;
    private final AsyncImporter               asyncImporter;
    private       ReferenceKeeper             referenceKeeper;

    public ConsumerBuilder(ReferenceKeeper referenceKeeper, Configuration applicationProperties, AtlasMetricsUtil metricsUtil, Map<String, Authentication> authnCache,
            AtlasEntityStore entityStore, AtlasInstanceConverter instanceConverter, EntityCorrelationManager entityCorrelationManager,
            AtlasTypeRegistry typeRegistry, Logger failedLog, Logger largeMessagesLog, AsyncImporter asyncImporter) {
        this.referenceKeeper          = referenceKeeper;
        this.applicationProperties    = applicationProperties;
        this.metricsUtil              = metricsUtil;
        this.authnCache               = authnCache;
        this.entityStore              = entityStore;
        this.instanceConverter        = instanceConverter;
        this.entityCorrelationManager = entityCorrelationManager;
        this.typeRegistry             = typeRegistry;
        this.failedLog                = failedLog;
        this.largeMessagesLog         = largeMessagesLog;
        this.asyncImporter            = asyncImporter;
    }

    @Override
    public Consumer build(BlockingQueue<Ticket> queue) {
        return new Consumer(
                this.referenceKeeper,
                new SerialEntityProcessor(applicationProperties, metricsUtil, authnCache,
                        entityStore, instanceConverter, entityCorrelationManager, typeRegistry,
                        failedLog, largeMessagesLog, asyncImporter),
                queue);
    }
}
