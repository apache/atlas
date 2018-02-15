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

package org.apache.atlas.omrs.archivemanager;

import org.apache.atlas.omrs.archivemanager.properties.*;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.*;

/**
 * OMRSArchiveBuilder creates an in-memory copy of an open metadata archive that can be saved to disk or processed
 * by a server.
 */
public class OMRSArchiveBuilder
{
    /*
     * Archive properties supplied on the constructor
     */
    private OpenMetadataArchiveProperties archiveProperties = null;

    /*
     * Hash maps for accumulating TypeDefs and instances as the content of the archive is built up.
     */
    private Map<String, PrimitiveDef>      primitiveDefMap      = new HashMap<>();
    private Map<String, CollectionDef>     collectionDefMap     = new HashMap<>();
    private Map<String, EnumDef>           enumDefMap           = new HashMap<>();
    private Map<String, ClassificationDef> classificationDefMap = new HashMap<>();
    private Map<String, EntityDef>         entityDefMap         = new HashMap<>();
    private Map<String, RelationshipDef>   relationshipDefMap   = new HashMap<>();
    private Map<String, TypeDefPatch>      typeDefPatchMap      = new HashMap<>();
    private Map<String, EntityDetail>      entityDetailMap      = new HashMap<>();
    private Map<String, Relationship>      relationshipMap      = new HashMap<>();


    /**
     * Typical constructor passes parameters used to build the open metadata archive's property header.
     *
     * @param archiveGUID - unique identifier for this open metadata archive.
     * @param archiveName - name of the open metadata archive.
     * @param archiveDescription - description of the open metadata archive.
     * @param archiveType - enum describing the type of archive this is.
     * @param originatorName - name of the originator (person or organization) of the archive.
     * @param creationDate - data that this archive was created.
     * @param dependsOnArchives - list of GUIDs for archives that this archive depends on (null for no dependencies).
     */
    public OMRSArchiveBuilder(String                  archiveGUID,
                              String                  archiveName,
                              String                  archiveDescription,
                              OpenMetadataArchiveType archiveType,
                              String                  originatorName,
                              Date                    creationDate,
                              ArrayList<String>       dependsOnArchives)
    {
        this.archiveProperties = new OpenMetadataArchiveProperties();

        this.archiveProperties.setArchiveGUID(archiveGUID);
        this.archiveProperties.setArchiveName(archiveName);
        this.archiveProperties.setArchiveDescription(archiveDescription);
        this.archiveProperties.setArchiveType(archiveType);
        this.archiveProperties.setOriginatorName(originatorName);
        this.archiveProperties.setCreationDate(creationDate);
        this.archiveProperties.setDependsOnArchives(dependsOnArchives);
    }


    /**
     * Add a new PrimitiveDef to the archive.
     *
     * @param primitiveDef - type to add
     */
    public void addPrimitiveDef(PrimitiveDef   primitiveDef)
    {
        if (primitiveDef != null)
        {
            primitiveDefMap.put(primitiveDef.getName(), primitiveDef);
        }
    }


    /**
     * Add a new CollectionDef to the archive.
     *
     * @param collectionDef - type to add
     */
    public void addCollectionDef(CollectionDef  collectionDef)
    {
        if (collectionDef != null)
        {
            collectionDefMap.put(collectionDef.getName(), collectionDef);
        }
    }


    /**
     * Add a new EnumDef to the archive.
     *
     * @param enumDef - type to add
     */
    public void addEnumDef(EnumDef    enumDef)
    {
        if (enumDef != null)
        {
            enumDefMap.put(enumDef.getName(), enumDef);
        }
    }


    /**
     * Add a new ClassificationDef to the archive.
     *
     * @param classificationDef - type to add
     */
    public void addClassificationDef(ClassificationDef   classificationDef)
    {
        if (classificationDef != null)
        {
            classificationDefMap.put(classificationDef.getName(), classificationDef);
        }
    }


    /**
     * Add a new EntityDef to the archive.
     *
     * @param entityDef - type to add
     */
    public void addEntityDef(EntityDef    entityDef)
    {
        if (entityDef != null)
        {
            EntityDef   previousDef = entityDefMap.put(entityDef.getName(), entityDef);

            if (previousDef != null)
            {
                // TODO log a duplicate
            }
        }
    }


    /**
     * Retrieve the entityDef - or null if it is not defined.
     *
     * @param entityDefName - name of the entity
     * @return the retrieved Entity def
     */
    public EntityDef  getEntityDef(String   entityDefName)
    {
        if (entityDefName != null)
        {
            EntityDef retrievedEntityDef = entityDefMap.get(entityDefName);

            if (retrievedEntityDef != null)
            {
                return retrievedEntityDef;
            }
            else
            {
                // TODO Throw exception
                return null; /* temporary */
            }
        }
        else
        {
            return null;
        }
    }


    /**
     * Add a new RelationshipDef to the archive.
     *
     * @param relationshipDef - type to add
     */
    public void addRelationshipDef(RelationshipDef   relationshipDef)
    {
        if (relationshipDef != null)
        {
            relationshipDefMap.put(relationshipDef.getName(), relationshipDef);
        }
    }


    /**
     * Add a new entity to the archive.
     *
     * @param entity - instance to add
     */
    public void addEntity(EntityDetail   entity)
    {
        if (entity != null)
        {
            entityDetailMap.put(entity.getGUID(), entity);
        }
    }


    /**
     * Add a new relationship to the archive.
     *
     * @param relationship - instance to add
     */
    public void addRelationship(Relationship  relationship)
    {
        if (relationship != null)
        {
            relationshipMap.put(relationship.getGUID(), relationship);
        }
    }


    /**
     * Once the content of the archive has been added to the archive builder, an archive object can be retrieved.
     *
     * @return open metadata archive object with all of the supplied content in it.
     */
    public OpenMetadataArchive  getOpenMetadataArchive()
    {
        OpenMetadataArchive    archive = new OpenMetadataArchive();

        /*
         * Set up the archive properties
         */
        archive.setArchiveProperties(this.archiveProperties);

        /*
         * Set up the TypeStore.  The types are added in a strict order to ensure that the dependencies are resolved.
         */
        ArrayList<AttributeTypeDef>  attributeTypeDefs = null;
        ArrayList<TypeDef>           typeDefs = new ArrayList<>();
        ArrayList<TypeDefPatch>      typeDefPatches = new ArrayList<>();

        if (! primitiveDefMap.isEmpty())
        {
            attributeTypeDefs.addAll(primitiveDefMap.values());
        }
        if (! collectionDefMap.isEmpty())
        {
            attributeTypeDefs.addAll(collectionDefMap.values());
        }
        if (! enumDefMap.isEmpty())
        {
            attributeTypeDefs.addAll(enumDefMap.values());
        }
        if (! entityDefMap.isEmpty())
        {
            typeDefs.addAll(entityDefMap.values());
        }
        if (! classificationDefMap.isEmpty())
        {
            typeDefs.addAll(classificationDefMap.values());
        }
        if (! relationshipDefMap.isEmpty())
        {
            typeDefs.addAll(relationshipDefMap.values());
        }

        if (! typeDefPatchMap.isEmpty())
        {
            typeDefPatches.addAll(typeDefPatchMap.values());
        }

        if ((! typeDefs.isEmpty()) || (! typeDefPatches.isEmpty()))
        {
            OpenMetadataArchiveTypeStore   typeStore = new OpenMetadataArchiveTypeStore();

            if (! attributeTypeDefs.isEmpty())
            {
                typeStore.setAttributeTypeDefs(attributeTypeDefs);
            }

            if (! typeDefs.isEmpty())
            {
                typeStore.setNewTypeDefs(typeDefs);
            }

            if (! typeDefPatches.isEmpty())
            {
                typeStore.setTypeDefPatches(typeDefPatches);
            }

            archive.setArchiveTypeStore(typeStore);
        }


        /*
         * Finally set up the instance store
         */
        ArrayList<EntityDetail>  entities      = new ArrayList<>();
        ArrayList<Relationship>  relationships = new ArrayList<>();

        if (! entityDetailMap.isEmpty())
        {
            entities.addAll(entityDetailMap.values());
        }
        if (! relationshipMap.isEmpty())
        {
            relationships.addAll(relationshipMap.values());
        }

        if ((! entities.isEmpty()) || (! relationships.isEmpty()))
        {
            OpenMetadataArchiveInstanceStore   instanceStore = new OpenMetadataArchiveInstanceStore();

            if (! entities.isEmpty())
            {
                instanceStore.setEntities(entities);
            }

            if (! relationships.isEmpty())
            {
                instanceStore.setRelationships(relationships);
            }

            archive.setArchiveInstanceStore(instanceStore);
        }

        return archive;
    }
}
