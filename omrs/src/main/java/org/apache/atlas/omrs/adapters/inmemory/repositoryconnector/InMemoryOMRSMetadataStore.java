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
package org.apache.atlas.omrs.adapters.inmemory.repositoryconnector;

import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityProxy;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;

import java.util.*;

/**
 * InMemoryOMRSMetadataStore provides the in memory stores for the InMemoryRepositoryConnector
 */
public class InMemoryOMRSMetadataStore
{
    private String                            repositoryName           = null;
    private HashMap<String, TypeDef>          typeDefStore             = new HashMap<>();
    private HashMap<String, AttributeTypeDef> attributeTypeDefStore    = new HashMap<>();
    private HashMap<String, EntityDetail>     entityStore              = new HashMap<>();
    private HashMap<String, EntityProxy>      entityProxyStore         = new HashMap<>();
    private List<EntityDetail>                entityHistoryStore       = new ArrayList<>();
    private HashMap<String, Relationship>     relationshipStore        = new HashMap<>();
    private List<Relationship>                relationshipHistoryStore = new ArrayList<>();


    /**
     * Default constructor
     */
    public InMemoryOMRSMetadataStore()
    {
    }


    /**
     * Set up the name of the repository for logging.
     *
     * @param repositoryName - String name
     */
    protected void  setRepositoryName(String    repositoryName)
    {
        this.repositoryName = repositoryName;
    }


    /**
     * Return a list of all of the defined AttributeTypeDefs.
     *
     * @return list of attribute type definitions
     */
    protected List<AttributeTypeDef> getAttributeTypeDefs()
    {
        return new ArrayList<>(attributeTypeDefStore.values());
    }


    /**
     * Return the AttributeTypeDef identified by the supplied guid.
     *
     * @param guid - unique identifier for the AttributeTypeDef
     * @return attribute type definition
     */
    protected AttributeTypeDef   getAttributeTypeDef(String  guid)
    {
        return attributeTypeDefStore.get(guid);
    }


    /**
     * Add an AttributeDefType to the store.
     *
     * @param attributeTypeDef - type to add
     */
    protected void  putAttributeTypeDef(AttributeTypeDef   attributeTypeDef)
    {
        attributeTypeDefStore.put(attributeTypeDef.getGUID(), attributeTypeDef);
    }


    /**
     * Return a list of all of the defined TypeDefs.
     *
     * @return list of type definitions
     */
    protected List<TypeDef>  getTypeDefs()
    {
        return new ArrayList<>(typeDefStore.values());
    }


    /**
     * Return the type definition identified by the guid.
     *
     * @param guid - unique identifier for type definition
     * @return type definition
     */
    protected TypeDef   getTypeDef(String guid)
    {
        return typeDefStore.get(guid);
    }


    /**
     * Add a type definition (TypeDef) to the store.
     *
     * @param typeDef - type definition
     */
    protected void  putTypeDef(TypeDef   typeDef)
    {
        typeDefStore.put(typeDef.getGUID(), typeDef);
    }


    /**
     * Return a list of entities from the store that are at the latest level.
     *
     * @return list of EntityDetail objects
     */
    protected List<EntityDetail>   getEntities()
    {
        return new ArrayList<>(entityStore.values());
    }


    /**
     * Return the entity identified by the guid.
     *
     * @param guid - unique identifier for the entity
     * @return entity object
     */
    protected EntityDetail  getEntity(String   guid)
    {
        return entityStore.get(guid);
    }


    /**
     * Return the entity proxy identified by the guid.
     *
     * @param guid - unique identifier
     * @return entity proxy object
     */
    protected EntityProxy  getEntityProxy(String   guid)
    {
        return entityProxyStore.get(guid);
    }


    /**
     * Return an entity store that contains entities as they were at the time supplied in the asOfTime
     * parameter
     *
     * @param asOfTime - time for the store (or null means now)
     * @return entity store for the requested time
     */
    protected HashMap<String, EntityDetail>  timeWarpEntityStore(Date         asOfTime)
    {
        if (asOfTime == null)
        {
            return entityStore;
        }

        HashMap<String, EntityDetail>  timeWarpedEntityStore = new HashMap<>();

        /*
         * First step through the current relationship store and extract all of the relationships that were
         * last updated before the asOfTime.
         */
        for (EntityDetail  entity : entityStore.values())
        {
            if (entity != null)
            {
                if (entity.getUpdateTime() != null)
                {
                    String entityGUID = entity.getGUID();

                    if (entityGUID != null)
                    {
                        if (! entity.getUpdateTime().after(asOfTime))
                        {
                            timeWarpedEntityStore.put(entityGUID, entity);
                        }
                    }
                }
                else if (entity.getCreateTime() != null)
                {
                    if (! entity.getCreateTime().after(asOfTime))
                    {
                        timeWarpedEntityStore.put(entity.getGUID(), entity);
                    }
                }
            }
        }

        /*
         * Now step through the history store picking up the versions of other entities that were active
         * at the time of the asOfTime.
         */
        for (EntityDetail oldEntity : entityHistoryStore)
        {
            if (oldEntity != null)
            {
                String entityGUID = oldEntity.getGUID();

                if (oldEntity.getUpdateTime() != null)
                {
                    if (! oldEntity.getUpdateTime().after(asOfTime))
                    {
                        EntityDetail newerEntity = timeWarpedEntityStore.put(entityGUID, oldEntity);

                        if (newerEntity != null)
                        {
                            timeWarpedEntityStore.put(entityGUID, newerEntity);
                        }
                        break;
                    }
                }
                else if (oldEntity.getCreateTime() != null)
                {
                    if (! oldEntity.getCreateTime().after(asOfTime))
                    {
                        timeWarpedEntityStore.put(entityGUID, oldEntity);
                        break;
                    }
                }
            }
        }

        return timeWarpedEntityStore;
    }


    /**
     * Return the list of relationships at their current level.
     *
     * @return list of relationships
     */
    protected List<Relationship>   getRelationships()
    {
        return new ArrayList<>(relationshipStore.values());
    }


    /**
     * Return the relationship identified by the guid.
     *
     * @param guid - unique identifier for the relationship
     * @return relationship object
     */
    protected Relationship  getRelationship(String   guid)
    {
        return relationshipStore.get(guid);
    }

    /**
     * Return a relationship store that contains relationships as they were at the time supplied in the asOfTime
     * parameter
     *
     * @param asOfTime - time for the store (or null means now)
     * @return relationship store for the requested time
     */
    protected HashMap<String, Relationship>  timeWarpRelationshipStore(Date         asOfTime)
    {
        if (asOfTime == null)
        {
            return relationshipStore;
        }


        HashMap<String, Relationship>  timeWarpedRelationshipStore = new HashMap<>();

        /*
         * First step through the current relationship store and extract all of the relationships that were
         * last updated before the asOfTime.
         */
        for (Relationship  relationship : relationshipStore.values())
        {
            if (relationship != null)
            {
                if (relationship.getUpdateTime() != null)
                {
                    String relationshipGUID = relationship.getGUID();

                    if (relationshipGUID != null)
                    {
                        if (! relationship.getUpdateTime().after(asOfTime))
                        {
                            timeWarpedRelationshipStore.put(relationshipGUID, relationship);
                        }
                    }
                }
                else if (relationship.getCreateTime() != null)
                {
                    if (! relationship.getCreateTime().after(asOfTime))
                    {
                        timeWarpedRelationshipStore.put(relationship.getGUID(), relationship);
                    }
                }
            }
        }

        /*
         * Now step through the history store picking up the versions of other relationships that were active
         * at the time of the asOfTime.
         */
        for (Relationship oldRelationship : relationshipHistoryStore)
        {
            if (oldRelationship != null)
            {
                String relationshipGUID = oldRelationship.getGUID();

                if (oldRelationship.getUpdateTime() != null)
                {
                    if (! oldRelationship.getUpdateTime().after(asOfTime))
                    {
                        Relationship newerRelationship = timeWarpedRelationshipStore.put(relationshipGUID, oldRelationship);

                        if (newerRelationship != null)
                        {
                            timeWarpedRelationshipStore.put(relationshipGUID, newerRelationship);
                        }
                        break;
                    }
                }
                else if (oldRelationship.getCreateTime() != null)
                {
                    if (! oldRelationship.getCreateTime().after(asOfTime))
                    {
                        timeWarpedRelationshipStore.put(relationshipGUID, oldRelationship);
                        break;
                    }
                }
            }
        }

        return timeWarpedRelationshipStore;
    }

    /**
     * Create a new entity in the entity store.
     *
     * @param entity - new version of the entity
     * @return entity with potentially updated GUID
     */
    protected EntityDetail createEntityInStore(EntityDetail    entity)
    {
        /*
         * There is a small chance the randomly generated GUID will clash with an existing relationship.
         * If this happens a new GUID is generated for the relationship and the process repeats.
         */
        EntityDetail existingEntity = entityStore.put(entity.getGUID(), entity);

        while (existingEntity != null)
        {
            entity.setGUID(UUID.randomUUID().toString());
            existingEntity = entityStore.put(entity.getGUID(), entity);
        }

        return entity;
    }


    /**
     * Create a new relationship in the relationship store.
     *
     * @param relationship - new version of the relationship
     * @return relationship with potentially updated GUID
     */
    protected Relationship createRelationshipInStore(Relationship    relationship)
    {
        /*
         * There is a small chance the randomly generated GUID will clash with an existing relationship.
         * If this happens a new GUID is generated for the relationship and the process repeats.
         */
        Relationship existingRelationship = relationshipStore.put(relationship.getGUID(), relationship);

        while (existingRelationship != null)
        {
            relationship.setGUID(UUID.randomUUID().toString());
            existingRelationship = relationshipStore.put(relationship.getGUID(), relationship);
        }

        return relationship;
    }


    /**
     * Save an entity proxy to the entity store.
     *
     * @param entityProxy - entity proxy object to add
     */
    protected void addEntityProxyToStore(EntityProxy    entityProxy)
    {
        entityProxyStore.put(entityProxy.getGUID(), entityProxy);
    }


    /**
     * Maintain a history of entities as they are stored into the entity store to ensure old version can be restored.
     * The history is maintained with the latest changes first in the list.
     *
     * @param entity - new version of the entity
     */
    protected void updateEntityInStore(EntityDetail    entity)
    {
        EntityDetail    oldEntity = entityStore.put(entity.getGUID(), entity);

        if (oldEntity != null)
        {
            entityHistoryStore.add(0, oldEntity);
        }
    }


    /**
     * Update an entity proxy in the proxy store.
     *
     * @param entityProxy - entity proxy object to add
     */
    protected void updateEntityProxyInStore(EntityProxy    entityProxy)
    {
        entityProxyStore.put(entityProxy.getGUID(), entityProxy);
    }



    /**
     * Maintain a history of relationships as they are stored into the relationship store to ensure old version
     * can be restored.  The history is maintained with the latest changes first in the list.
     *
     * @param relationship - new version of the relationship
     */
    protected void updateRelationshipInStore(Relationship    relationship)
    {
        Relationship    oldRelationship = relationshipStore.put(relationship.getGUID(), relationship);

        if (oldRelationship != null)
        {
            relationshipHistoryStore.add(0, oldRelationship);
        }
    }


    /**
     * Save a reference copy of an entity to the active store.  Reference copies are not maintained in the
     * history store.
     *
     * @param entity - object to save
     */
    protected void saveReferenceEntityToStore(EntityDetail    entity)
    {
        entityStore.put(entity.getGUID(), entity);
    }


    /**
     * Save a reference copy of a relationship to the active store.  Reference copies are not maintained in the
     * history store.
     *
     * @param relationship - object to save
     */
    protected void saveReferenceRelationshipToStore(Relationship    relationship)
    {
        relationshipStore.put(relationship.getGUID(), relationship);
    }


    /**
     * Retrieve the previous version of a Relationship.  This is the first instance of this element that
     * appears in the history.
     *
     * @param guid - unique identifier for the required element
     * @return - previous version of this relationship - or null if not found
     */
    protected Relationship retrievePreviousVersionOfRelationship(String   guid)
    {
        if (guid != null)
        {
            int  elementPosition = 0;

            for (Relationship relationship : relationshipHistoryStore)
            {
                if (relationship != null)
                {
                    if (guid.equals(relationship.getGUID()))
                    {
                        relationshipHistoryStore.remove(elementPosition);
                        relationshipStore.put(guid, relationship);
                        return relationship;
                    }
                }

                elementPosition ++;
            }
        }

        return null;
    }


    /**
     * Retrieve the previous version of an Entity from the history store and restore it in the entity store.
     * This is the first instance of this element that appears in the history.
     *
     * @param guid - unique identifier for the required element
     * @return - previous version of this Entity - or null if not found
     */
    protected EntityDetail retrievePreviousVersionOfEntity(String   guid)
    {
        if (guid != null)
        {
            int  elementPosition = 0;

            for (EntityDetail entity : entityHistoryStore)
            {
                if (entity != null)
                {
                    if (guid.equals(entity.getGUID()))
                    {
                        entityHistoryStore.remove(elementPosition);
                        entityStore.put(guid, entity);
                        return entity;
                    }
                }

                elementPosition ++;
            }
        }

        return null;
    }


    /**
     * Remove an entity from the active store and add it to the history store.
     *
     * @param entity - entity to remove
     */
    protected void removeEntityFromStore(EntityDetail     entity)
    {
        entityStore.remove(entity.getGUID());
        entityHistoryStore.add(0, entity);
    }

    /**
     * Remove a reference entity from the active store and add it to the history store.
     *
     * @param guid - entity to remove
     */
    protected void removeReferenceEntityFromStore(String     guid)
    {
        EntityDetail entity = entityStore.remove(guid);

        if (entity != null)
        {
            entityHistoryStore.add(0, entity);
        }
    }


    /**
     * Remove an entity from the active store and add it to the history store.
     *
     * @param guid - entity proxy to remove
     */
    protected void removeEntityProxyFromStore(String     guid)
    {
        entityProxyStore.remove(guid);
    }


    /**
     * Remove a relationship from the active store and add it to the history store.
     *
     * @param relationship - relationship to remove
     */
    protected void removeRelationshipFromStore(Relationship     relationship)
    {
        relationshipStore.remove(relationship.getGUID());
        relationshipHistoryStore.add(0, relationship);
    }


    /**
     * Remove a reference relationship from the active store and add it to the history store.
     *
     * @param guid - relationship to remove
     */
    protected void removeReferenceRelationshipFromStore(String     guid)
    {
        Relationship  relationship = relationshipStore.remove(guid);

        if (relationship != null)
        {
            relationshipHistoryStore.add(0, relationship);
        }
    }

}
