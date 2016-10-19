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
package org.apache.atlas.store;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumDefs;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.util.List;

/**
 * Interface to persistence store of TypeDef
 */
public interface AtlasTypeDefStore {
    void init() throws AtlasBaseException;

    /***********************/
    /** EnumDef operation **/
    /***********************/

    AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException;

    List<AtlasEnumDef> getAllEnumDefs() throws AtlasBaseException;

    AtlasEnumDef getEnumDefByName(String name) throws AtlasBaseException;

    AtlasEnumDef getEnumDefByGuid(String guid) throws AtlasBaseException;

    AtlasEnumDef updateEnumDefByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException;

    AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException;

    void deleteEnumDefByName(String name) throws AtlasBaseException;

    void deleteEnumDefByGuid(String guid) throws AtlasBaseException;

    AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException;

    /*************************/
    /** StructDef operation **/
    /*************************/
    AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException;

    List<AtlasStructDef> getAllStructDefs() throws AtlasBaseException;

    AtlasStructDef getStructDefByName(String name) throws AtlasBaseException;

    AtlasStructDef getStructDefByGuid(String guid) throws AtlasBaseException;

    AtlasStructDef updateStructDefByName(String name, AtlasStructDef structDef) throws AtlasBaseException;

    AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException;

    void deleteStructDefByName(String name) throws AtlasBaseException;

    void deleteStructDefByGuid(String guid) throws AtlasBaseException;

    AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException;


    /*********************************/
    /** ClassificationDef operation **/
    /*********************************/
    AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef) throws AtlasBaseException;

    List<AtlasClassificationDef> getAllClassificationDefs() throws AtlasBaseException;

    AtlasClassificationDef getClassificationDefByName(String name) throws AtlasBaseException;

    AtlasClassificationDef getClassificationDefByGuid(String guid) throws AtlasBaseException;

    AtlasClassificationDef updateClassificationDefByName(String name, AtlasClassificationDef classificationDef)
            throws AtlasBaseException;

    AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef)
            throws AtlasBaseException;

    void deleteClassificationDefByName(String name) throws AtlasBaseException;

    void deleteClassificationDefByGuid(String guid) throws AtlasBaseException;

    AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException;


    /*************************/
    /** EntityDef operation **/
    /*************************/
    AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException;

    List<AtlasEntityDef> getAllEntityDefs() throws AtlasBaseException;

    AtlasEntityDef getEntityDefByName(String name) throws AtlasBaseException;

    AtlasEntityDef getEntityDefByGuid(String guid) throws AtlasBaseException;

    AtlasEntityDef updateEntityDefByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException;

    AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException;

    void deleteEntityDefByName(String name) throws AtlasBaseException;

    void deleteEntityDefByGuid(String guid) throws AtlasBaseException;

    AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException;

    /***** Bulk Operations *****/

    AtlasTypesDef createTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    AtlasTypesDef updateTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    void deleteTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    AtlasTypesDef searchTypesDef(SearchFilter searchFilter) throws AtlasBaseException;
}
