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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import java.util.List;

public class AtlasEntityStreamForImport extends AtlasEntityStream implements EntityImportStream {
    public AtlasEntityStreamForImport(AtlasEntity entity) {
        super(entity);
    }

    public AtlasEntityStreamForImport(AtlasEntity entity, EntityStream entityStream) {
        super(entity, entityStream);
    }

    @Override
    public void onImportComplete(String guid) {

    }
}
