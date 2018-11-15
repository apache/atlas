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
import org.apache.atlas.repository.graph.DeleteHandler;
import org.apache.atlas.repository.graph.HardDeleteHandler;
import org.apache.atlas.repository.graph.SoftDeleteHandler;
import org.apache.atlas.store.DeleteType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@Component
public class DeleteHandlerDelegate {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteHandlerDelegate.class);

    private final SoftDeleteHandler softDeleteHandler;
    private final HardDeleteHandler hardDeleteHandler;
    private final DeleteHandler     defaultHandler;

    @Inject
    public DeleteHandlerDelegate(TypeSystem typeSystem) {
        this.softDeleteHandler = new SoftDeleteHandler(typeSystem);
        this.hardDeleteHandler = new HardDeleteHandler(typeSystem);
        this.defaultHandler    = getDefaultConfiguredHandler(typeSystem);
    }

    public DeleteHandler getHandler() {
        return getHandler(RequestContextV1.get().getDeleteType());
    }

    public DeleteHandler getHandler(DeleteType deleteType) {
        if (deleteType == null) {
            deleteType = DeleteType.DEFAULT;
        }

        switch (deleteType) {
            case SOFT:
                return softDeleteHandler;

            case HARD:
                return hardDeleteHandler;

            default:
                return defaultHandler;
        }
    }

    private DeleteHandler getDefaultConfiguredHandler(TypeSystem typeSystem) {
        DeleteHandler ret = null;

        try {
            Class handlerFromProperties = AtlasRepositoryConfiguration.getDeleteHandlerImpl();

            LOG.info("Default delete handler set to: {}", handlerFromProperties.getName());

            ret = (DeleteHandler) handlerFromProperties.getConstructor(TypeSystem.class).newInstance(typeSystem);
        } catch (Exception ex) {
            LOG.error("Error instantiating default delete handler. Defaulting to: {}", softDeleteHandler.getClass().getName(), ex);

            ret = softDeleteHandler;
        }

        return ret;
    }
}
