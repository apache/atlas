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

import java.util.Collections;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;

import com.google.inject.Singleton;

/**
 * Implementation that completely disables the audit repository.
 */
@Singleton
public class NoopEntityAuditRepository implements EntityAuditRepository {

    @Override
    public void putEvents(EntityAuditEvent... events) throws AtlasException {
        //do nothing
    }

    @Override
    public synchronized void putEvents(List<EntityAuditEvent> events) throws AtlasException {
        //do nothing
    }

    @Override
    public List<EntityAuditEvent> listEvents(String entityId, String startKey, short maxResults)
            throws AtlasException {
        return Collections.emptyList();
    }
}
