/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.listener;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;

import java.util.ArrayList;
import java.util.List;

public class ChangedTypeDefs {
    private List<? extends AtlasBaseTypeDef> createTypeDefs;
    private List<? extends AtlasBaseTypeDef> updatedTypeDefs;
    private List<? extends AtlasBaseTypeDef> deletedTypeDefs;

    public ChangedTypeDefs(List<? extends AtlasBaseTypeDef> createTypeDefs,
                           List<? extends AtlasBaseTypeDef> updatedTypeDefs,
                           List<? extends AtlasBaseTypeDef> deletedTypeDefs) {
        this.createTypeDefs = createTypeDefs;
        this.updatedTypeDefs = updatedTypeDefs;
        this.deletedTypeDefs = deletedTypeDefs;
    }

    public ChangedTypeDefs() {
        createTypeDefs = new ArrayList<>();
        updatedTypeDefs = new ArrayList<>();
        deletedTypeDefs = new ArrayList<>();
    }

    public List<? extends AtlasBaseTypeDef> getCreateTypeDefs() {
        return createTypeDefs;
    }

    public ChangedTypeDefs setCreateTypeDefs(List<? extends AtlasBaseTypeDef> createTypeDefs) {
        this.createTypeDefs = createTypeDefs;
        return this;
    }

    public List<? extends AtlasBaseTypeDef> getUpdatedTypeDefs() {
        return updatedTypeDefs;
    }

    public ChangedTypeDefs setUpdatedTypeDefs(List<? extends AtlasBaseTypeDef> updatedTypeDefs) {
        this.updatedTypeDefs = updatedTypeDefs;
        return this;
    }

    public List<? extends AtlasBaseTypeDef> getDeletedTypeDefs() {
        return deletedTypeDefs;
    }

    public ChangedTypeDefs setDeletedTypeDefs(List<? extends AtlasBaseTypeDef> deletedTypeDefs) {
        this.deletedTypeDefs = deletedTypeDefs;
        return this;
    }
}
