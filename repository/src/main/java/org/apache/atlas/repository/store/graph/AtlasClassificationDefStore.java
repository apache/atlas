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
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasClassificationDef;

import java.util.List;

/**
 * Interface for graph persistence store for AtlasClassificationDef
 */
public interface AtlasClassificationDefStore {
    Object preCreate(AtlasClassificationDef classificationDef) throws AtlasBaseException;

    AtlasClassificationDef create(AtlasClassificationDef classifiDef, Object preCreateResult) throws AtlasBaseException;

    List<AtlasClassificationDef> getAll() throws AtlasBaseException;

    AtlasClassificationDef getByName(String name) throws AtlasBaseException;

    AtlasClassificationDef getByGuid(String guid) throws AtlasBaseException;

    AtlasClassificationDef update(AtlasClassificationDef classifiDef) throws AtlasBaseException;

    AtlasClassificationDef updateByName(String name, AtlasClassificationDef classifiDef) throws AtlasBaseException;

    AtlasClassificationDef updateByGuid(String guid, AtlasClassificationDef classifiDef) throws AtlasBaseException;

    Object preDeleteByName(String name) throws AtlasBaseException;

    void deleteByName(String name, Object preDeleteResult) throws AtlasBaseException;

    Object preDeleteByGuid(String guid) throws AtlasBaseException;

    void deleteByGuid(String guid, Object preDeleteResult) throws AtlasBaseException;
}
