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

package org.apache.atlas.repository.typestore;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.TypeSystem;

public interface ITypeStore {

    /**
     * Add types to the underlying type storage layer
     * @param typeSystem {@link TypeSystem} object which contains existing types. To lookup newly added types,
     *                                     an instance of {@link TypeSystem.TransientTypeSystem} can be passed.
     * @param types names of newly added types.
     * @throws AtlasException
     */
    void store(TypeSystem typeSystem, ImmutableList<String> types) throws AtlasException;

    /**
     * Restore all type definitions
     * @return List of persisted type definitions
     * @throws AtlasException
     */
    TypesDef restore() throws AtlasException;
}
