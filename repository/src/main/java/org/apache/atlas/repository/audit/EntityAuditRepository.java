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

package org.apache.atlas.repository.audit;

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;

import java.util.List;

/**
 * Interface for repository for storing entity audit events
 */
public interface EntityAuditRepository {
    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    void putEvents(EntityAuditEvent... events) throws AtlasException;

    /**
     * Add events to the event repository
     * @param events events to be added
     * @throws AtlasException
     */
    void putEvents(List<EntityAuditEvent> events) throws AtlasException;

    /**
     * List events for the given entity id in decreasing order of timestamp, from the given timestamp. Returns n results
     * @param entityId entity id
     * @param startKey key for the first event to be returned, used for pagination
     * @param n number of events to be returned
     * @return list of events
     * @throws AtlasException
     */
    List<EntityAuditEvent> listEvents(String entityId, String startKey, short n) throws AtlasException;
}
