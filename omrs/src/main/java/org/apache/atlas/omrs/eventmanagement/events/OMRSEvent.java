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


import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1;
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1ErrorSection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceProvenanceType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * OMRSEvent defines the common content of a message that is sent through the OMRSTopicConnector to all metadata
 * repositories registered in the open metadata repository cohort.  It supports a category enum for the three
 * main categories of event and provides specialized structures for processing each category of event.
 */
public abstract class OMRSEvent
{
    /*
     * Basic event header information.
     */
    protected Date                         eventTimestamp       = null;
    protected OMRSEventDirection           eventDirection       = null;

    /*
     * The category of the event.
     */
    protected  OMRSEventCategory           eventCategory        = OMRSEventCategory.UNKNOWN;

    /*
     * Information about the originator of the event.
     */
    protected OMRSEventOriginator          eventOriginator      = null;

    /*
     * Specific variables only used in error reporting
     */
    protected OMRSEventErrorCode     genericErrorCode           = null;
    protected String                 errorMessage               = null;
    protected String                 targetMetadataCollectionId = null;
    protected Connection             targetRemoteConnection     = null;
    protected TypeDefSummary         targetTypeDefSummary       = null;
    protected AttributeTypeDef       targetAttributeTypeDef     = null;
    protected String                 targetInstanceGUID         = null;
    protected InstanceProvenanceType otherOrigin                = null;
    protected String                 otherMetadataCollectionId  = null;
    protected TypeDefSummary         otherTypeDefSummary        = null;
    protected TypeDef                otherTypeDef               = null;
    protected AttributeTypeDef       otherAttributeTypeDef      = null;
    protected String                 otherInstanceGUID          = null;

    private static final Logger log = LoggerFactory.getLogger(OMRSEvent.class);


    /**
     * Inbound event constructor that takes the object created by the Jackson JSON mapper and unpacks the
     * properties into the internal OMRSEvent object.
     *
     * @param inboundEvent - incoming Event.
     */
    public OMRSEvent(OMRSEventV1 inboundEvent)
    {
        this.eventDirection = OMRSEventDirection.INBOUND;

        if (inboundEvent != null)
        {
            this.eventTimestamp = inboundEvent.getTimestamp();
            this.eventOriginator = inboundEvent.getOriginator();
            this.eventCategory = inboundEvent.getEventCategory();

            OMRSEventV1ErrorSection errorSection = inboundEvent.getErrorSection();

            if (errorSection != null)
            {
                genericErrorCode = errorSection.getErrorCode();
                errorMessage = errorSection.getErrorMessage();
                targetMetadataCollectionId = errorSection.getTargetMetadataCollectionId();
                targetRemoteConnection = errorSection.getTargetRemoteConnection();
                targetTypeDefSummary = errorSection.getTargetTypeDefSummary();
                targetAttributeTypeDef = errorSection.getTargetAttributeTypeDef();
                targetInstanceGUID = errorSection.getTargetInstanceGUID();
                otherOrigin = errorSection.getOtherOrigin();
                otherMetadataCollectionId = errorSection.getOtherMetadataCollectionId();
                otherTypeDefSummary = errorSection.getOtherTypeDefSummary();
                otherTypeDef = errorSection.getOtherTypeDef();
                otherAttributeTypeDef = errorSection.getOtherAttributeTypeDef();
                otherInstanceGUID = errorSection.getOtherInstanceGUID();
            }
        }
    }


    /**
     * Outbound event constructor used when there is no error.
     *
     * @param eventCategory - category of event.
     */
    public OMRSEvent(OMRSEventCategory    eventCategory)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;
    }


    /**
     * Outbound event constructor used for registry error events.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - identifier of the metadata collection in error.
     * @param targetRemoteConnection - connection used to create the connector to access metadata in the
     *                               remote repository.
     */
    public OMRSEvent(OMRSEventCategory  eventCategory,
                     OMRSEventErrorCode genericErrorCode,
                     String             errorMessage,
                     String             targetMetadataCollectionId,
                     Connection         targetRemoteConnection)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetRemoteConnection = targetRemoteConnection;
    }


    /**
     * Outbound constructor used for TypeDef conflict events.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - identifier of the metadata collection required to change TypeDef.
     * @param targetTypeDefSummary - details of TypeDef to change.
     * @param otherTypeDefSummary - description of conflicting TypeDef that will not change.
     */
    public OMRSEvent(OMRSEventCategory  eventCategory,
                     OMRSEventErrorCode genericErrorCode,
                     String             errorMessage,
                     String             targetMetadataCollectionId,
                     TypeDefSummary     targetTypeDefSummary,
                     TypeDefSummary     otherTypeDefSummary)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetTypeDefSummary = targetTypeDefSummary;
        this.otherTypeDefSummary = otherTypeDefSummary;
    }


    /**
     * Outbound constructor used for AttributeTypeDef conflict events.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - identifier of the metadata collection required to change TypeDef.
     * @param targetAttributeTypeDef - details of AttrbuteTypeDef to change.
     * @param otherAttributeTypeDef - description of conflicting AttributeTypeDef that will not change.
     */
    public OMRSEvent(OMRSEventCategory  eventCategory,
                     OMRSEventErrorCode genericErrorCode,
                     String             errorMessage,
                     String             targetMetadataCollectionId,
                     AttributeTypeDef   targetAttributeTypeDef,
                     AttributeTypeDef   otherAttributeTypeDef)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetAttributeTypeDef = targetAttributeTypeDef;
        this.otherAttributeTypeDef = otherAttributeTypeDef;
    }


    /**
     * Outbound event constructor for a TypeDef patch mismatch warning.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error.
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - identifier of the remote metadata collection with mismatched TypeDef.
     * @param targetTypeDefSummary - description of TypeDef.
     * @param otherTypeDef - details of local TypeDef
     */
    public OMRSEvent(OMRSEventCategory  eventCategory,
                     OMRSEventErrorCode genericErrorCode,
                     String             errorMessage,
                     String             targetMetadataCollectionId,
                     TypeDefSummary     targetTypeDefSummary,
                     TypeDef            otherTypeDef)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetTypeDefSummary = targetTypeDefSummary;
        this.otherTypeDef = otherTypeDef;
    }

    /**
     * Outbound constructor used for metadata instance conflict events.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - description of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherOrigin - origin of the other (older) metadata instance
     * @param otherMetadataCollectionId - metadata collection of the other (older) metadata instance
     * @param otherTypeDefSummary - details of the other (older) instance's TypeDef
     * @param otherInstanceGUID - unique identifier for the other (older) instance
     */
    public OMRSEvent(OMRSEventCategory      eventCategory,
                     OMRSEventErrorCode     genericErrorCode,
                     String                 errorMessage,
                     String                 targetMetadataCollectionId,
                     TypeDefSummary         targetTypeDefSummary,
                     String                 targetInstanceGUID,
                     String                 otherMetadataCollectionId,
                     InstanceProvenanceType otherOrigin,
                     TypeDefSummary         otherTypeDefSummary,
                     String                 otherInstanceGUID)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetTypeDefSummary = targetTypeDefSummary;
        this.targetInstanceGUID = targetInstanceGUID;
        this.otherMetadataCollectionId = otherMetadataCollectionId;
        this.otherOrigin = otherOrigin;
        this.otherTypeDefSummary = otherTypeDefSummary;
        this.otherInstanceGUID = otherInstanceGUID;
    }

    /**
     * Outbound constructor used for metadata instance type conflict events.
     *
     * @param eventCategory - category of event.
     * @param genericErrorCode - code for the error
     * @param errorMessage - detailed error message for remote audit log
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - details of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherTypeDefSummary - details of the local TypeDef
     */
    public OMRSEvent(OMRSEventCategory      eventCategory,
                     OMRSEventErrorCode     genericErrorCode,
                     String                 errorMessage,
                     String                 targetMetadataCollectionId,
                     TypeDefSummary         targetTypeDefSummary,
                     String                 targetInstanceGUID,
                     TypeDefSummary         otherTypeDefSummary)
    {
        this.eventDirection = OMRSEventDirection.OUTBOUND;
        this.eventTimestamp = new Date();
        this.eventCategory = eventCategory;

        this.genericErrorCode = genericErrorCode;
        this.errorMessage = errorMessage;
        this.targetMetadataCollectionId = targetMetadataCollectionId;
        this.targetTypeDefSummary = targetTypeDefSummary;
        this.targetInstanceGUID = targetInstanceGUID;
        this.otherTypeDefSummary = otherTypeDefSummary;
    }


    /**
     * Set up details of the event originator - used by the event publisher for outbound events.
     *
     * @param eventOriginator  - details of the originator of the event including the id of the local
     *                         metadata collection.
     */
    public void setEventOriginator(OMRSEventOriginator eventOriginator)
    {
        this.eventOriginator = eventOriginator;
    }


    /**
     * Return whether this is an inbound or outbound event.  This is used for messages.
     *
     * @return OMRSEventDirection enum
     */
    public OMRSEventDirection getEventDirection()
    {
        return eventDirection;
    }


    /**
     * Return the timestamp for the event.
     *
     * @return Date object
     */
    public Date getEventTimestamp()
    {
        return eventTimestamp;
    }


    /**
     * Return the category of the event. If the event category is null then the event was unreadable
     * in some form (or there is a logic error).
     *
     * @return event category enum
     */
    public OMRSEventCategory getEventCategory()
    {
        return eventCategory;
    }


    /**
     * Return details of the originator of the event including the id of their metadata collection.
     * If the originator is null then the event was unreadable in some form (or there is a logic error).
     *
     * @return event originator object
     */
    public OMRSEventOriginator getEventOriginator()
    {
        return eventOriginator;
    }


    /**
     * Return the error code for the event.  This is set to null if there is no error.
     *
     * @return error code enum or null
     */
    protected OMRSEventErrorCode getGenericErrorCode()
    {
        return genericErrorCode;
    }


    /**
     * Return any error message for the event.  This is null if there is no error.  If there is an error, this
     * error message is suitable for the local OMRS audit log.
     *
     * @return String errorMessage
     */
    public String getErrorMessage()
    {
        return errorMessage;
    }


    /**
     * This is the identifier of the metadata collection that needs to take action.
     * It is null if there is no error condition.
     *
     * @return String metadata collection id
     */
    public String getTargetMetadataCollectionId()
    {
        return targetMetadataCollectionId;
    }


    /**
     * This is the target's connection that is causing errors in the originator's server.
     *
     * @return OCF connection
     */
    public Connection getTargetRemoteConnection()
    {
        return targetRemoteConnection;
    }


    /**
     * Return the target's TypeDef summary.
     *
     * @return TypeDefSummary containing identifiers, category and version
     */
    public TypeDefSummary getTargetTypeDefSummary()
    {
        return targetTypeDefSummary;
    }


    /**
     * Return the target AttributeTypeDef.
     *
     * @return AttributeTypeDef object
     */
    public AttributeTypeDef getTargetAttributeTypeDef()
    {
        return targetAttributeTypeDef;
    }


    /**
     * Return the target's instance's unique identifier.
     *
     * @return String guid
     */
    public String getTargetInstanceGUID()
    {
        return targetInstanceGUID;
    }


    /**
     * Return the provenance (origin) information for the other instance.
     *
     * @return InstanceProvenanceType enum
     */
    public InstanceProvenanceType getOtherOrigin()
    {
        return otherOrigin;
    }


    /**
     * Return the unique identifier for the metadata collection containing the other instance.
     *
     * @return String guid
     */
    public String getOtherMetadataCollectionId()
    {
        return otherMetadataCollectionId;
    }


    /**
     * Return the version of the TypeDef from the other repository.
     *
     * @return TypeDefSummary containing identifiers, category and version
     */
    public TypeDefSummary getOtherTypeDefSummary()
    {
        return otherTypeDefSummary;
    }


    /**
     * Return the TypeDef from the other repository.
     *
     * @return TypeDef object
     */
    public TypeDef getOtherTypeDef()
    {
        return otherTypeDef;
    }


    /**
     * Return the AttributeTypeDef from the other repository.
     *
     * @return AttributeTypeDef object
     */
    public AttributeTypeDef getOtherAttributeTypeDef()
    {
        return otherAttributeTypeDef;
    }

    /**
     * Return the unique identifier for the other instance.
     *
     * @return String guid
     */
    public String getOtherInstanceGUID()
    {
        return otherInstanceGUID;
    }


    /**
     * Returns an OMRSEvent populated with details about a generic event.  Specific subtypes override this method
     * to create messages with specific subsections.
     *
     * @return OMRSEvent (Version 1) object
     */
    public OMRSEventV1  getOMRSEventV1()
    {
        OMRSEventV1     omrsEvent = new OMRSEventV1();

        omrsEvent.setTimestamp(this.eventTimestamp);
        omrsEvent.setOriginator(this.eventOriginator);
        omrsEvent.setEventCategory(this.eventCategory);

        if (this.genericErrorCode != null)
        {
            OMRSEventV1ErrorSection errorSection = new OMRSEventV1ErrorSection();

            errorSection.setErrorCode(this.genericErrorCode);
            errorSection.setErrorMessage(this.errorMessage);
            errorSection.setTargetMetadataCollectionId(this.targetMetadataCollectionId);
            errorSection.setTargetRemoteConnection(this.targetRemoteConnection);
            errorSection.setTargetTypeDefSummary(this.targetTypeDefSummary);
            errorSection.setTargetAttributeTypeDef(this.targetAttributeTypeDef);
            errorSection.setTargetInstanceGUID(this.targetInstanceGUID);
            errorSection.setOtherMetadataCollectionId(this.otherMetadataCollectionId);
            errorSection.setOtherOrigin(this.otherOrigin);
            errorSection.setOtherTypeDefSummary(this.otherTypeDefSummary);
            errorSection.setOtherTypeDef(this.otherTypeDef);
            errorSection.setOtherAttributeTypeDef(this.otherAttributeTypeDef);
            errorSection.setOtherInstanceGUID(this.otherInstanceGUID);

            omrsEvent.setErrorSection(errorSection);
        }

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
        return "OMRSEvent{" +
                "eventTimestamp=" + eventTimestamp +
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
