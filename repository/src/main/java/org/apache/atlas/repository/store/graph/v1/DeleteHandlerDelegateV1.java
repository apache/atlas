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

package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.RequestContextV1;
import org.apache.atlas.store.DeleteType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@Component
public class DeleteHandlerDelegateV1 {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteHandlerDelegateV1.class);

    private final SoftDeleteHandlerV1 softDeleteHandlerV1;
    private final HardDeleteHandlerV1 hardDeleteHandlerV1;
    private final DeleteHandlerV1     defaultHandlerV1;

    @Inject
    public DeleteHandlerDelegateV1(AtlasTypeRegistry typeRegistry) {
        this.softDeleteHandlerV1 = new SoftDeleteHandlerV1(typeRegistry);
        this.hardDeleteHandlerV1 = new HardDeleteHandlerV1(typeRegistry);
        this.defaultHandlerV1    = getDefaultConfiguredHandlerV1(typeRegistry);
    }

    public DeleteHandlerV1 getHandlerV1() {
        return getHandlerV1(RequestContextV1.get().getDeleteType());
    }

    public DeleteHandlerV1 getHandlerV1(DeleteType deleteType) {
        if (deleteType == null) {
            deleteType = DeleteType.DEFAULT;
        }

        switch (deleteType) {
            case SOFT:
                return softDeleteHandlerV1;

            case HARD:
                return hardDeleteHandlerV1;

            default:
                return defaultHandlerV1;
        }
    }

    private DeleteHandlerV1 getDefaultConfiguredHandlerV1(AtlasTypeRegistry typeRegistry) {
        DeleteHandlerV1 ret = null;

        try {
            Class handlerFromProperties = AtlasRepositoryConfiguration.getDeleteHandlerV1Impl();

            LOG.info("Default delete handler set to: {}", handlerFromProperties.getName());

            ret = (DeleteHandlerV1) handlerFromProperties.getConstructor(AtlasTypeRegistry.class).newInstance(typeRegistry);
        } catch (Exception ex) {
            LOG.error("Error instantiating default delete handler. Defaulting to: {}", softDeleteHandlerV1.getClass().getName(), ex);

            ret = softDeleteHandlerV1;
        }

        return ret;
    }
}
