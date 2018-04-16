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
package org.apache.atlas.omrs.eventmanagement.events;


import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1;
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1InstanceSection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceProvenanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;

import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OMRSInstanceEvent extends OMRSEvent
{
    /*
     * The type of the registry event that defines how the rest of the values should be interpreted.
     */
    private  OMRSInstanceEventType       instanceEventType = OMRSInstanceEventType.UNKNOWN_INSTANCE_EVENT;

    /*
     * Instance specific properties for typical instance events
     */
    private String       typeDefGUID    = null;
    private String       typeDefName    = null;
    private String       instanceGUID   = null;
    private EntityDetail entity         = null;
    private Relationship relationship   = null;

    /*
     * Home repository Id for refresh requests.
     */
    private String homeMetadataCollectionId = null;

    /*
     * Instance specific properties for events related to correcting conflicts in the open metadata repository
     * cohort.
     */
    private String         originalHomeMetadataCollectionId = null;
    private TypeDefSummary originalTypeDefSummary           = null;
    private String         originalInstanceGUID             = null;

    /*
     * Specific variables only used in error reporting.  It defines the subset of error codes from OMRSEvent
     * that are specific to instance events.
     */
    private  OMRSInstanceEventErrorCode  errorCode  = OMRSInstanceEventErrorCode.NOT_IN_USE;


    private static final Logger log = LoggerFactory.getLogger(OMRSInstanceEvent.class);

    /**
     * Inbound event constructor that takes the object created by the Jackson JSON mapper and unpacks the
     * properties into the instance event.
     *
     * @param inboundEvent - incoming event to parse.
     */
    public OMRSInstanceEvent(OMRSEventV1 inboundEvent)
    {
        super(inboundEvent);

        OMRSEventV1InstanceSection instanceSection = inboundEvent.getInstanceEventSection();

        if (instanceSection != null)
        {
            this.instanceEventType = instanceSection.getEventType();

            this.typeDefGUID = instanceSection.getTypeDefGUID();
            this.typeDefName = instanceSection.getTypeDefName();
            this.instanceGUID = instanceSection.getInstanceGUID();
            this.entity = instanceSection.getEntity();
            this.relationship = instanceSection.getRelationship();
            this.homeMetadataCollectionId = instanceSection.getHomeMetadataCollectionId();

            this.originalHomeMetadataCollectionId = instanceSection.getOriginalHomeMetadataCollectionId();
            this.originalTypeDefSummary = instanceSection.getOriginalTypeDefSummary();
            this.originalInstanceGUID = instanceSection.getOriginalInstanceGUID();
        }

        if (super.genericErrorCode != null)
        {
            switch(genericErrorCode)
            {
                case CONFLICTING_INSTANCES:
                    errorCode = OMRSInstanceEventErrorCode.CONFLICTING_INSTANCES;
                    break;

                case CONFLICTING_TYPE:
                    errorCode = OMRSInstanceEventErrorCode.CONFLICTING_TYPE;
                    break;

                default:
                    errorCode = OMRSInstanceEventErrorCode.UNKNOWN_ERROR_CODE;
                    break;
            }
        }
    }


    /**
     * Constructor for instance events related to a change to an entity.
     *
     * @param instanceEventType - type of event
     * @param entity - entity that changed
     */
    public OMRSInstanceEvent(OMRSInstanceEventType instanceEventType, EntityDetail entity)
    {
        super(OMRSEventCategory.INSTANCE);

        this.instanceEventType = instanceEventType;
        this.entity = entity;
    }


    /**
     * Constructor for instance events related to a change to a relationship.
     *
     * @param instanceEventType - type of event
     * @param relationship - relationship that changed
     */
    public OMRSInstanceEvent(OMRSInstanceEventType instanceEventType, Relationship relationship)
    {
        super(OMRSEventCategory.INSTANCE);

        this.instanceEventType = instanceEventType;
        this.relationship = relationship;
    }


    /**
     * Constructor for instance events related to a delete or purge of an instance - or a request to refresh
     * an instance.
     *
     * @param instanceEventType - type of event
     * @param typeDefGUID - unique identifier for this entity's TypeDef
     * @param typeDefName - name of this entity's TypeDef
     * @param instanceGUID - unique identifier for the entity
     */
    public OMRSInstanceEvent(OMRSInstanceEventType instanceEventType,
                             String                typeDefGUID,
                             String                typeDefName,
                             String                instanceGUID)
    {
        super(OMRSEventCategory.INSTANCE);

        this.instanceEventType = instanceEventType;
        this.typeDefGUID = typeDefGUID;
        this.typeDefName = typeDefName;
        this.instanceGUID = instanceGUID;
    }


    /**
     * Constructor for instance conflict events.
     *
     * @param errorCode - error code
     * @param errorMessage - description of the error
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - details of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherMetadataCollectionId - local metadata collection id
     * @param otherOrigin - provenance information of the local instance
     * @param otherTypeDefSummary - TypeDef details of the local instance
     * @param otherInstanceGUID - GUID of the local instance
     */
    public OMRSInstanceEvent(OMRSInstanceEventErrorCode errorCode,
                             String                     errorMessage,
                             String                     targetMetadataCollectionId,
                             TypeDefSummary             targetTypeDefSummary,
                             String                     targetInstanceGUID,
                             String                     otherMetadataCollectionId,
                             InstanceProvenanceType     otherOrigin,
                             TypeDefSummary             otherTypeDefSummary,
                             String                     otherInstanceGUID)
    {
        super(OMRSEventCategory.INSTANCE,
              errorCode.getErrorCodeEncoding(),
              errorMessage,
              targetMetadataCollectionId,
              targetTypeDefSummary,
              targetInstanceGUID,
              otherMetadataCollectionId,
              otherOrigin,
              otherTypeDefSummary,
              otherInstanceGUID);

        this.errorCode = errorCode;
    }


    /**
     * Instance type conflict event.
     *
     * @param errorCode - error code
     * @param errorMessage - description of the error
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - details of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherTypeDefSummary - details of the other TypeDef
     */
    public OMRSInstanceEvent(OMRSInstanceEventErrorCode errorCode,
                             String                     errorMessage,
                             String                     targetMetadataCollectionId,
                             TypeDefSummary             targetTypeDefSummary,
                             String                     targetInstanceGUID,
                             TypeDefSummary             otherTypeDefSummary)
    {
        super(OMRSEventCategory.INSTANCE,
              errorCode.getErrorCodeEncoding(),
              errorMessage,
              targetMetadataCollectionId,
              targetTypeDefSummary,
              targetInstanceGUID,
              otherTypeDefSummary);

        this.errorCode = errorCode;
    }

    /**
     * Set up the home metadata collection Id - used for when a repository is requesting a refresh of an instance's
     * details.
     *
     * @param homeMetadataCollectionId - unique id of the metadata collection where this instance comes from.
     */
    public void setHomeMetadataCollectionId(String homeMetadataCollectionId)
    {
        this.homeMetadataCollectionId = homeMetadataCollectionId;
    }


    /**
     * Set up the unique id of the metadata collection that was the original home of a metadata instance that
     * has just been rehomed.
     *
     * @param originalHomeMetadataCollectionId unique id of original metadata collection
     */
    public void setOriginalHomeMetadataCollectionId(String originalHomeMetadataCollectionId)
    {
        this.originalHomeMetadataCollectionId = originalHomeMetadataCollectionId;
    }


    /**
     * Set up the details of the original TypeDef of a metadata instance that has just been reTyped.
     *
     * @param originalTypeDefSummary - details of original TypeDef
     */
    public void setOriginalTypeDefSummary(TypeDefSummary originalTypeDefSummary)
    {
        this.originalTypeDefSummary = originalTypeDefSummary;
    }


    /**
     * Set up the original unique id (guid) of an instance that has just been re-identified (ie it has
     * had a new guid assigned.
     *
     * @param originalInstanceGUID - original guid of an instance
     */
    public void setOriginalInstanceGUID(String originalInstanceGUID)
    {
        this.originalInstanceGUID = originalInstanceGUID;
    }


    /**
     * Return the code for this event's type.
     *
     * @return OMRSInstanceEventType enum
     */
    public OMRSInstanceEventType getInstanceEventType()
    {
        return instanceEventType;
    }


    /**
     * Return the unique identifier for the instance's TypeDef.
     *
     * @return String identifier (guid)
     */
    public String getTypeDefGUID()
    {
        return typeDefGUID;
    }


    /**
     * Return the unique name for the instance's TypeDef.
     *
     * @return String name
     */
    public String getTypeDefName()
    {
        return typeDefName;
    }


    /**
     * Return the unique identifier for the instance itself.
     *
     * @return String identifier (guid)
     */
    public String getInstanceGUID()
    {
        return instanceGUID;
    }


    /**
     * Return the entity instance (if applicable) or null.
     *
     * @return EntityDetail object
     */
    public EntityDetail getEntity()
    {
        return entity;
    }


    /**
     * Return the relationship instance (if applicable) or null.
     *
     * @return Relationship object
     */
    public Relationship getRelationship()
    {
        return relationship;
    }


    /**
     * Return the identifier of the instance's home metadata collection.  This is used on refresh requests.
     *
     * @return String unique identifier (guid)
     */
    public String getHomeMetadataCollectionId()
    {
        return homeMetadataCollectionId;
    }

    /**
     * Return the identifier of the original metadata collection for this instance.  This is used when an
     * instance is being re-homed.
     *
     * @return String unique identifier (guid)
     */
    public String getOriginalHomeMetadataCollectionId()
    {
        return originalHomeMetadataCollectionId;
    }


    /**
     * Return the original version for this instance's TypeDef.  This is used if the type for the
     * instance has been changed to resolve a conflict or to allow a change in the TypeDef Gallery.
     *
     * @return details of the original TypeDef
     */
    public TypeDefSummary getOriginalTypeDefSummary()
    {
        return originalTypeDefSummary;
    }


    /**
     * Return the original unique identifier (guid) for this instance.  This is used if the guid for the instance
     * has been changed to resolve a conflict.
     *
     * @return String unique identifier (guid)
     */
    public String getOriginalInstanceGUID()
    {
        return originalInstanceGUID;
    }


    /**
     * Return the error code for this instance event.  If there is no error it is set to NOT_IN_USE.
     *
     * @return OMRSInstanceEventErrorCode enum
     */
    public OMRSInstanceEventErrorCode getErrorCode()
    {
        return errorCode;
    }


    /**
     * Returns an OMRSEvent populated with details from this InstanceEvent
     *
     * @return OMRSEvent (Version 1) object
     */
    public OMRSEventV1  getOMRSEventV1()
    {
        OMRSEventV1     omrsEvent = super.getOMRSEventV1();

        OMRSEventV1InstanceSection instanceSection  = new OMRSEventV1InstanceSection();

        instanceSection.setEventType(this.instanceEventType);

        instanceSection.setTypeDefGUID(this.typeDefGUID);
        instanceSection.setTypeDefName(this.typeDefName);
        instanceSection.setInstanceGUID(this.instanceGUID);
        instanceSection.setEntity(this.entity);
        instanceSection.setRelationship(this.relationship);
        instanceSection.setHomeMetadataCollectionId(this.homeMetadataCollectionId);

        instanceSection.setOriginalHomeMetadataCollectionId(this.originalHomeMetadataCollectionId);
        instanceSection.setOriginalTypeDefSummary(this.originalTypeDefSummary);
        instanceSection.setOriginalInstanceGUID(this.originalInstanceGUID);

        omrsEvent.setInstanceEventSection(instanceSection);

        return omrsEvent;
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "OMRSInstanceEvent{" +
                "instanceEventType=" + instanceEventType +
                ", typeDefGUID='" + typeDefGUID + '\'' +
                ", typeDefName='" + typeDefName + '\'' +
                ", instanceGUID='" + instanceGUID + '\'' +
                ", entity=" + entity +
                ", relationship=" + relationship +
                ", homeMetadataCollectionId='" + homeMetadataCollectionId + '\'' +
                ", originalHomeMetadataCollectionId='" + originalHomeMetadataCollectionId + '\'' +
                ", originalTypeDefSummary=" + originalTypeDefSummary +
                ", originalInstanceGUID='" + originalInstanceGUID + '\'' +
                ", errorCode=" + errorCode +
                ", eventTimestamp=" + eventTimestamp +
                ", eventDirection=" + eventDirection +
                ", eventCategory=" + eventCategory +
                ", eventOriginator=" + eventOriginator +
                ", genericErrorCode=" + genericErrorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", targetMetadataCollectionId='" + targetMetadataCollectionId + '\'' +
                ", targetRemoteConnection=" + targetRemoteConnection +
                ", targetTypeDefSummary=" + targetTypeDefSummary +
                ", targetAttributeTypeDef=" + targetAttributeTypeDef +
                ", targetInstanceGUID='" + targetInstanceGUID + '\'' +
                ", otherOrigin=" + otherOrigin +
                ", otherMetadataCollectionId='" + otherMetadataCollectionId + '\'' +
                ", otherTypeDefSummary=" + otherTypeDefSummary +
                ", otherTypeDef=" + otherTypeDef +
                ", otherAttributeTypeDef=" + otherAttributeTypeDef +
                ", otherInstanceGUID='" + otherInstanceGUID + '\'' +
                '}';
    }
}
