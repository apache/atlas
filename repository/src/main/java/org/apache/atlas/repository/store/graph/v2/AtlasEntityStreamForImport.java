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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;

public class AtlasEntityStreamForImport extends AtlasEntityStream implements EntityImportStream {
    private int currentPosition = 0;

    public AtlasEntityStreamForImport(AtlasEntityWithExtInfo entityWithExtInfo, EntityStream entityStream) {
        super(entityWithExtInfo, entityStream);
    }

    @Override
    public AtlasEntityWithExtInfo getNextEntityWithExtInfo() {
        currentPosition++;
        AtlasEntity entity = next();

        return entity != null ? new AtlasEntityWithExtInfo(entity, super.entitiesWithExtInfo) : null;
    }

    @Override
    public AtlasEntity getByGuid(String guid) {
        AtlasEntity ent = super.entitiesWithExtInfo.getEntity(guid);

        if(ent == null && entityStream != null) {
            return entityStream.getByGuid(guid);
        }

        return ent;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public void setPosition(int position) {
        // not applicable for a single entity stream
    }

    @Override
    public int getPosition() {
        return currentPosition;
    }

    @Override
    public void setPositionUsingEntityGuid(String guid) {
    }

    @Override
    public void onImportComplete(String guid) {

    }
}
