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
package org.apache.atlas.omas.assetconsumer.publisher;


import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omas.assetconsumer.properties.Asset;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * AssetConsumerPublisher is responsible for publishing events about assets.  It is called
 * when an interesting OMRS Event is added to the Enterprise OMRS Topic.  It adds events to the Asset Consumer OMAS
 * out topic.
 */
public class AssetConsumerPublisher
{
    private static final String assetTypeName                  = "Asset";
    private static final String assetPropertyNameQualifiedName = "qualifiedName";
    private static final String assetPropertyNameDisplayName   = "name";
    private static final String assetPropertyNameOwner         = "owner";
    private static final String assetPropertyNameDescription   = "description";

    private static final Logger log = LoggerFactory.getLogger(AssetConsumerPublisher.class);

    private Connection              assetConsumerOutTopic;
    private OMRSRepositoryHelper    repositoryHelper;
    private OMRSRepositoryValidator repositoryValidator;
    private String                  componentName;


    /**
     * The constructor is given the connection to the out topic for Asset Consumer OMAS
     * along with classes for testing and manipulating instances.
     *
     * @param assetConsumerOutTopic - connection to the out topic
     * @param repositoryHelper - provides methods for working with metadata instances
     * @param repositoryValidator - provides validation of metadata instance
     * @param componentName - name of component
     */
    public AssetConsumerPublisher(Connection              assetConsumerOutTopic,
                                  OMRSRepositoryHelper    repositoryHelper,
                                  OMRSRepositoryValidator repositoryValidator,
                                  String                  componentName)
    {
        this.assetConsumerOutTopic = assetConsumerOutTopic;
        this.repositoryHelper = repositoryHelper;
        this.repositoryValidator = repositoryValidator;
        this.componentName = componentName;
    }


    /**
     * Determine whether a new entity is an Asset.  If it is then publish an Asset Consumer Event about it.
     *
     * @param entity - entity object that has just been created.
     */
    public void processNewEntity(EntityDetail   entity)
    {
        String assetType = getAssetType(entity);

        if (assetType != null)
        {
            this.processNewAsset(this.getAsset(entity));
        }
    }


    /**
     * Determine whether an updated entity is an Asset.  If it is then publish an Asset Consumer Event about it.
     *
     * @param entity - entity object that has just been updated.
     */
    public void processUpdatedEntity(EntityDetail   entity)
    {
        String assetType = getAssetType(entity);

        if (assetType != null)
        {
            this.processUpdatedAsset(this.getAsset(entity));
        }
    }


    /**
     * Determine whether a deleted entity is an Asset.  If it is then publish an Asset Consumer Event about it.
     *
     * @param entity - entity object that has just been deleted.
     */
    public void processDeletedEntity(EntityDetail   entity)
    {
        String assetType = getAssetType(entity);

        if (assetType != null)
        {
            this.processDeletedAsset(this.getAsset(entity));
        }
    }


    /**
     * Determine whether a restored entity is an Asset.  If it is then publish an Asset Consumer Event about it.
     *
     * @param entity - entity object that has just been restored.
     */
    public void processRestoredEntity(EntityDetail   entity)
    {
        String assetType = getAssetType(entity);

        if (assetType != null)
        {
            this.processRestoredAsset(this.getAsset(entity));
        }
    }


    /**
     * Determine whether a new relationship is related to an Asset.
     * If it is then publish an Asset Consumer Event about it.
     *
     * @param relationship - relationship object that has just been created.
     */
    public void processNewRelationship(Relationship   relationship)
    {
        // todo
    }


    /**
     * Determine whether an updated relationship is related to an Asset.
     * If it is then publish an Asset Consumer Event about it.
     *
     * @param relationship - relationship object that has just been updated.
     */
    public void processUpdatedRelationship(Relationship   relationship)
    {
        // todo
    }


    /**
     * Determine whether a deleted relationship is related to an Asset.
     * If it is then publish an Asset Consumer Event about it.
     *
     * @param relationship - relationship object that has just been deleted.
     */
    public void processDeletedRelationship(Relationship   relationship)
    {
        // todo
    }


    /**
     * Determine whether a restored relationship is related to an Asset.
     * If it is then publish an Asset Consumer Event about it.
     *
     * @param relationship - relationship object that has just been restored.
     */
    public void processRestoredRelationship(Relationship   relationship)
    {
        // todo
    }


    /**
     * Return the name of the Asset type if this entity has a type that inherits from Asset.
     *
     * @param entity - entity to test
     * @return String containing Asset type name, or null if not an Asset.
     */
    private String getAssetType(EntityDetail  entity)
    {
        final   String   methodName = "getAssetType";

        if (repositoryValidator.isATypeOf(componentName, entity, assetTypeName, methodName))
        {
            InstanceType   entityType = entity.getType();

            if (entityType != null)
            {
                return entityType.getTypeDefName();
            }
        }

        return null;
    }


    /**
     * Return an Asset object extracted from the supplied entity object
     * @param entity - entity describing the asset
     * @return Asset object
     */
    private Asset getAsset(EntityDetail   entity)
    {
        Asset   asset = new Asset();

        if (entity != null)
        {
            asset.setURL(entity.getInstanceURL());
            asset.setGUID(entity.getGUID());

            InstanceType  instanceType = entity.getType();
            if (instanceType != null)
            {
                asset.setTypeId(instanceType.getTypeDefGUID());
                asset.setTypeName(instanceType.getTypeDefName());
                asset.setTypeVersion(instanceType.getTypeDefVersion());
                asset.setTypeDescription(instanceType.getTypeDefDescription());
            }

            InstanceProperties instanceProperties = entity.getProperties();

            if (instanceProperties != null)
            {
                InstancePropertyValue  instancePropertyValue;

                instancePropertyValue = instanceProperties.getPropertyValue(assetPropertyNameQualifiedName);

                if (instancePropertyValue != null)
                {
                    PrimitivePropertyValue  primitivePropertyValue = (PrimitivePropertyValue)instancePropertyValue;
                    asset.setQualifiedName(primitivePropertyValue.toString());
                }

                instancePropertyValue = instanceProperties.getPropertyValue(assetPropertyNameDisplayName);

                if (instancePropertyValue != null)
                {
                    PrimitivePropertyValue  primitivePropertyValue = (PrimitivePropertyValue)instancePropertyValue;
                    asset.setDisplayName(primitivePropertyValue.toString());
                }

                instancePropertyValue = instanceProperties.getPropertyValue(assetPropertyNameOwner);

                if (instancePropertyValue != null)
                {
                    PrimitivePropertyValue  primitivePropertyValue = (PrimitivePropertyValue)instancePropertyValue;
                    asset.setOwner(primitivePropertyValue.toString());
                }

                instancePropertyValue = instanceProperties.getPropertyValue(assetPropertyNameDescription);

                if (instancePropertyValue != null)
                {
                    PrimitivePropertyValue  primitivePropertyValue = (PrimitivePropertyValue)instancePropertyValue;
                    asset.setDescription(primitivePropertyValue.toString());
                }
            }
        }

        return asset;
    }


    /**
     * Publish event about a new asset.
     *
     * @param asset - asset to report on.
     */
    private void processNewAsset(Asset   asset)
    {
        log.info("Asset Consumer Event => New Asset: " + asset.toString());
    }


    /**
     * Publish event about an updated asset.
     *
     * @param asset - asset to report on.
     */
    private void processUpdatedAsset(Asset   asset)
    {
        log.info("Asset Consumer Event => Updated Asset: " + asset.toString());
    }


    /**
     * Publish event about a deleted asset.
     *
     * @param asset - asset to report on.
     */
    private void processDeletedAsset(Asset   asset)
    {
        log.info("Asset Consumer Event => Deleted Asset: " + asset.toString());
    }


    /**
     * Publish event about a restored asset.
     *
     * @param asset - asset to report on.
     */
    private void processRestoredAsset(Asset   asset)
    {
        log.info("Asset Consumer Event => Restored Asset: " + asset.toString());
    }
}
