/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;

import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;

public class SoftDeleteHandlerV1 extends DeleteHandlerV1 {
    @Inject
    public SoftDeleteHandlerV1(AtlasGraph graph, AtlasTypeRegistry typeRegistry, TaskManagement taskManagement) {
        super(graph, typeRegistry, false, true, taskManagement);
    }

    @Override
    protected void _deleteVertex(AtlasVertex instanceVertex, boolean force) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SoftDeleteHandlerV1._deleteVertex({}, {})", GraphHelper.string(instanceVertex), force);
        }

        if (force) {
            graphHelper.removeVertex(instanceVertex);
        } else {
            Status state = AtlasGraphUtilsV2.getState(instanceVertex);

            if (state != DELETED) {
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, STATE_PROPERTY_KEY, DELETED.name());
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFIED_BY_KEY, RequestContext.get().getUser());

                // Store DELETE event Kafka message creation time for accurate lineage correlation
                // This is the actual time when DELETE event was published in Kafka (not when Atlas processed it)
                Long deleteEventMsgCreationTime = RequestContext.get().getDeleteEventMsgTime();
                if (deleteEventMsgCreationTime != null) {
                    AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, Constants.ENTITY_DELETE_EVENT_TIME_PROPERTY_KEY, deleteEventMsgCreationTime);
                    LOG.debug("Stored DELETE event Kafka message creation time {} for deleted entity {}", deleteEventMsgCreationTime, GraphHelper.getGuid(instanceVertex));
                }
            }
        }
    }

    @Override
    protected void deleteEdge(AtlasEdge edge, boolean force) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SoftDeleteHandlerV1.deleteEdge({}, {})", GraphHelper.string(edge), force);
        }

        if (force) {
            removeTagPropagation(edge);

            graphHelper.removeEdge(edge);
        } else {
            Status state = AtlasGraphUtilsV2.getState(edge);

            if (state != DELETED) {
                AtlasGraphUtilsV2.setEncodedProperty(edge, STATE_PROPERTY_KEY, DELETED.name());
                AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
                AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFIED_BY_KEY, RequestContext.get().getUser());
            }
        }
    }
}
