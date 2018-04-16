/*
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
package org.apache.atlas.omrs.localrepository.repositorycontentmanager;

import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityProxy;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntitySummary;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;


/**
 * OMRSInstanceValidator provides method to validate entities and relationships match their type definition
 * (TypeDef).
 */
public interface OMRSInstanceValidator
{
    /**
     * Test that the supplied entity is valid.
     *
     * @param sourceName - source of the entity (used for logging)
     * @param entity - entity to test
     * @return boolean result
     */
    boolean validEntity(String        sourceName,
                        EntitySummary entity);


    /**
     * Test that the supplied entity is valid.
     *
     * @param sourceName - source of the entity (used for logging)
     * @param entity - entity to test
     * @return boolean result
     */
    boolean validEntity(String      sourceName,
                        EntityProxy entity);


    /**
     * Test that the supplied entity is valid.
     *
     * @param sourceName - source of the entity (used for logging)
     * @param entity - entity to test
     * @return boolean result
     */
    boolean validEntity(String       sourceName,
                        EntityDetail entity);


    /**
     * Test that the supplied relationship is valid.
     *
     * @param sourceName - source of the relationship (used for logging)
     * @param relationship - relationship to test
     * @return boolean result
     */
    boolean validRelationship(String       sourceName,
                              Relationship relationship);

    /**
     * Verify that the identifiers for an instance are correct.
     *
     * @param sourceName - source of the instance (used for logging)
     * @param typeDefGUID - unique identifier for the type.
     * @param typeDefName - unique name for the type.
     * @param category - expected category of the instance.
     * @param instanceGUID - unique identifier for the instance.
     * @return boolean indicating whether the identifiers are ok.
     */
    boolean validInstanceId(String           sourceName,
                            String           typeDefGUID,
                            String           typeDefName,
                            TypeDefCategory  category,
                            String           instanceGUID);
}
