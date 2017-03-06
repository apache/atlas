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

package org.apache.atlas.repository.memory;

import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.IConstructableType;

@Deprecated
public interface IAttributeStore {
    /**
     * Store the attribute's value from the 'instance' into this store.
     * @param pos
     * @param instance
     * @throws RepositoryException
     */
    void store(int pos, IConstructableType type, StructInstance instance) throws RepositoryException;

    /**
     * load the Instance with the value from position 'pos' for the attribute.
     * @param pos
     * @param instance
     * @throws RepositoryException
     */
    void load(int pos, IConstructableType type, StructInstance instance) throws RepositoryException;

    /**
     * Ensure store have space for the given pos.
     * @param pos
     * @throws RepositoryException
     */
    void ensureCapacity(int pos) throws RepositoryException;
}
