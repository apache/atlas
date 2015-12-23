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

package org.apache.falcon.atlas.service;

import org.apache.atlas.falcon.hook.FalconHook;
import org.apache.falcon.FalconException;
import org.apache.falcon.atlas.Util.EventUtil;
import org.apache.falcon.atlas.event.FalconEvent;
import org.apache.falcon.atlas.publisher.FalconEventPublisher;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Atlas service to publish Falcon events
 */
public class AtlasService implements FalconService, ConfigurationChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasService.class);
    private FalconEventPublisher publisher;

    /**
     * Constant for the service name.
     */
    public static final String SERVICE_NAME = AtlasService.class.getSimpleName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        ConfigurationStore.get().registerListener(this);
        publisher = new FalconHook();
    }


    @Override
    public void destroy() throws FalconException {
        ConfigurationStore.get().unregisterListener(this);
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        EntityType entityType = entity.getEntityType();
        switch (entityType) {
            case PROCESS:
                addProcessEntity((Process) entity, FalconEvent.OPERATION.ADD_PROCESS);
                break;

            default:
                LOG.debug("Entity type not processed " + entityType);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        EntityType entityType = newEntity.getEntityType();
        switch (entityType) {
            case PROCESS:
                addProcessEntity((Process) newEntity, FalconEvent.OPERATION.UPDATE_PROCESS);
                break;

            default:
                LOG.debug("Entity type not processed " + entityType);
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        //Since there is no import script that can import existing falcon entities to atlas, adding on falcon service start
        onAdd(entity);
    }

    private void addProcessEntity(Process entity, FalconEvent.OPERATION operation) throws FalconException {
        LOG.info("Adding process entity to Atlas: {}", entity.getName());

        try {
            String user = entity.getACL() != null ? entity.getACL().getOwner() :
                    UserGroupInformation.getLoginUser().getShortUserName();
            FalconEvent event = new FalconEvent(user, EventUtil.getUgi(), operation, System.currentTimeMillis(), entity);
            FalconEventPublisher.Data data = new FalconEventPublisher.Data(event);
            publisher.publish(data);
        } catch (Exception ex) {
            throw new FalconException("Unable to publish data to publisher " + ex.getMessage(), ex);
        }
    }
}
