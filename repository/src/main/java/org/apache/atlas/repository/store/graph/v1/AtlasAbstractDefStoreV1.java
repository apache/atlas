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

import org.apache.atlas.type.AtlasTypeRegistry;

/**
 * Abstract typedef-store for v1 format.
 */
public abstract class AtlasAbstractDefStoreV1 {
    protected final AtlasTypeDefGraphStoreV1 typeDefStore;
    protected final AtlasTypeRegistry        typeRegistry;

    public AtlasAbstractDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
    }
}
