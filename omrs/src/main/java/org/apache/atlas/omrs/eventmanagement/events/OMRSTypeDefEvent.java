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
import org.apache.atlas.omrs.eventmanagement.events.v1.OMRSEventV1TypeDefSection;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.AttributeTypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;

import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefPatch;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OMRSTypeDefEvent extends OMRSEvent
{
    /*
     * The type of the TypeDef event that defines how the rest of the values should be interpreted.
     */
    private OMRSTypeDefEventType typeDefEventType = OMRSTypeDefEventType.UNKNOWN_TYPEDEF_EVENT;

    /*
     * TypeDef specific properties.
     */
    private AttributeTypeDef attributeTypeDef = null;
    private TypeDef          typeDef          = null;
    private String           typeDefGUID      = null;
    private String           typeDefName      = null;
    private TypeDefPatch     typeDefPatch     = null;

    /*
     * TypeDef specific properties for events related to correcting conflicts in the open metadata repository
     * cohort.
     */
    private TypeDefSummary   originalTypeDefSummary   = null;
    private AttributeTypeDef originalAttributeTypeDef = null;

    /*
     * Specific variables only used in error reporting.  It defines the subset of error codes from OMRSEvent
     * that are specific to TypeDef events.
     */
    private OMRSTypeDefEventErrorCode   errorCode                  = OMRSTypeDefEventErrorCode.NOT_IN_USE;


    private static final Logger log = LoggerFactory.getLogger(OMRSTypeDefEvent.class);

    /**
     * Inbound event constructor that takes the object created by the Jackson JSON mapper and unpacks the
     * properties into the instance event.
     *
     * @param inboundEvent - incoming Event.
     */
    public OMRSTypeDefEvent(OMRSEventV1 inboundEvent)
    {
        super(inboundEvent);

        OMRSEventV1TypeDefSection typeDefSection = inboundEvent.getTypeDefEventSection();

        if (typeDefSection != null)
        {
            this.typeDefEventType = typeDefSection.getTypeDefEventType();
            this.attributeTypeDef = typeDefSection.getAttributeTypeDef();
            this.typeDef = typeDefSection.getTypeDef();
            this.typeDefGUID = typeDefSection.getTypeDefGUID();
            this.typeDefName = typeDefSection.getTypeDefName();
            this.typeDefPatch = typeDefSection.getTypeDefPatch();
            this.originalTypeDefSummary = typeDefSection.getOriginalTypeDefSummary();
            this.originalAttributeTypeDef = typeDefSection.getOriginalAttributeTypeDef();
        }

        if (super.genericErrorCode != null)
        {
            switch(genericErrorCode)
            {
                case CONFLICTING_TYPEDEFS:
                    errorCode = OMRSTypeDefEventErrorCode.CONFLICTING_TYPEDEFS;
                    break;

                case CONFLICTING_ATTRIBUTE_TYPEDEFS:
                    errorCode = OMRSTypeDefEventErrorCode.CONFLICTING_ATTRIBUTE_TYPEDEFS;

                case TYPEDEF_PATCH_MISMATCH:
                    errorCode = OMRSTypeDefEventErrorCode.TYPEDEF_PATCH_MISMATCH;
                    break;

                default:
                    errorCode = OMRSTypeDefEventErrorCode.UNKNOWN_ERROR_CODE;
                    break;
            }
        }
    }

    /**
     * Outbound event constructor for events such as newTypeDef.
     *
     * @param typeDefEventType - type of event
     * @param typeDef - Complete details of the TypeDef that is the subject of the event.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            TypeDef              typeDef)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.typeDef = typeDef;
    }


    /**
     * Outbound event constructor for events such as newAttributeTypeDef.
     *
     * @param typeDefEventType - type of event
     * @param attributeTypeDef - Complete details of the AttributeTypeDef that is the subject of the event.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            AttributeTypeDef     attributeTypeDef)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.attributeTypeDef = attributeTypeDef;
    }


    /**
     * Outbound event constructor for events such as updates.
     *
     * @param typeDefEventType - type of event
     * @param typeDefPatch - Complete details of the TypeDef that is the subject of the event.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            TypeDefPatch         typeDefPatch)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.typeDefPatch = typeDefPatch;
    }


    /**
     * Outbound event constructor for events such as deletes.
     *
     * @param typeDefEventType - type of event
     * @param typeDefGUID - Unique identifier of the TypeDef that is the subject of the event.
     * @param typeDefName - Unique name of the TypeDef that is the subject of the event.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            String               typeDefGUID,
                            String               typeDefName)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.typeDefGUID = typeDefGUID;
        this.typeDefName = typeDefName;
    }


    /**
     * Outbound event constructor for changing the identifiers associated with TypeDefs.
     *
     * @param typeDefEventType - type of event
     * @param originalTypeDefSummary - description of the original TypeDef that is the subject of the event.
     * @param typeDef - updated TypeDef with new identifiers
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            TypeDefSummary       originalTypeDefSummary,
                            TypeDef              typeDef)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.originalTypeDefSummary = originalTypeDefSummary;
        this.typeDef = typeDef;
    }


    /**
     * Outbound event constructor for changing the identifiers associated with AttributeTypeDefs.
     *
     * @param typeDefEventType - type of event
     * @param originalAttributeTypeDef - description of the original AttributeTypeDef that is the subject of the event.
     * @param attributeTypeDef - updated AttributeTypeDef with new identifiers
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventType typeDefEventType,
                            AttributeTypeDef     originalAttributeTypeDef,
                            AttributeTypeDef     attributeTypeDef)
    {
        super(OMRSEventCategory.TYPEDEF);

        this.typeDefEventType = typeDefEventType;
        this.originalAttributeTypeDef = originalAttributeTypeDef;
        this.attributeTypeDef = attributeTypeDef;
    }


    /**
     * Outbound event constructor for conflicting typedef errors.
     *
     * @param errorCode - code enum indicating the cause of the error.
     * @param errorMessage - descriptive message about the error.
     * @param targetMetadataCollectionId - identifier of the cohort member that issued the event in error.
     * @param targetTypeDefSummary - details of the TypeDef in the remote repository.
     * @param otherTypeDefSummary - details of the TypeDef in the local repository.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventErrorCode errorCode,
                            String                    errorMessage,
                            String                    targetMetadataCollectionId,
                            TypeDefSummary            targetTypeDefSummary,
                            TypeDefSummary            otherTypeDefSummary)
    {
        super(OMRSEventCategory.TYPEDEF,
              errorCode.getErrorCodeEncoding(),
              errorMessage,
              targetMetadataCollectionId,
              targetTypeDefSummary,
              otherTypeDefSummary);

        this.typeDefEventType = OMRSTypeDefEventType.TYPEDEF_ERROR_EVENT;
    }


    /**
     * Outbound event constructor for conflicting attribute typedef errors.
     *
     * @param errorCode - code enum indicating the cause of the error.
     * @param errorMessage - descriptive message about the error.
     * @param targetMetadataCollectionId - identifier of the cohort member that issued the event in error.
     * @param targetAttributeTypeDef - details of the TypeDef in the remote repository.
     * @param otherAttributeTypeDef - details of the TypeDef in the local repository.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventErrorCode errorCode,
                            String                    errorMessage,
                            String                    targetMetadataCollectionId,
                            AttributeTypeDef          targetAttributeTypeDef,
                            AttributeTypeDef          otherAttributeTypeDef)
    {
        super(OMRSEventCategory.TYPEDEF,
              errorCode.getErrorCodeEncoding(),
              errorMessage,
              targetMetadataCollectionId,
              targetAttributeTypeDef,
              targetAttributeTypeDef);

        this.typeDefEventType = OMRSTypeDefEventType.TYPEDEF_ERROR_EVENT;
    }



    /**
     * Outbound event constructor for typedef mismatch errors.
     *
     * @param errorCode - code enum indicating the cause of the error.
     * @param errorMessage - descriptive message about the error.
     * @param targetMetadataCollectionId - identifier of the cohort member that issued the event in error.
     * @param targetTypeDefSummary - details of the TypeDef in the remote repository.
     * @param otherTypeDef - details of the TypeDef in the local repository.
     */
    public OMRSTypeDefEvent(OMRSTypeDefEventErrorCode errorCode,
                            String                    errorMessage,
                            String                    targetMetadataCollectionId,
                            TypeDefSummary            targetTypeDefSummary,
                            TypeDef                   otherTypeDef)
    {
        super(OMRSEventCategory.TYPEDEF,
              errorCode.getErrorCodeEncoding(),
              errorMessage,
              targetMetadataCollectionId,
              targetTypeDefSummary,
              otherTypeDef);

        this.typeDefEventType = OMRSTypeDefEventType.TYPEDEF_ERROR_EVENT;
    }


    /**
     * Return the code for this event's type.
     *
     * @return OMRSTypeDefEventType enum
     */
    public OMRSTypeDefEventType getTypeDefEventType()
    {
        return typeDefEventType;
    }


    /**
     * Return the complete TypeDef object.
     *
     * @return TypeDef object
     */
    public TypeDef getTypeDef()
    {
        return typeDef;
    }


    /**
     * Return the complete AttributeTypeDef object.
     *
     * @return AttributeTypeDef object
     */
    public AttributeTypeDef getAttributeTypeDef()
    {
        return attributeTypeDef;
    }

    /**
     * Return the unique id of the TypeDef.
     *
     * @return String guid
     */
    public String getTypeDefGUID()
    {
        return typeDefGUID;
    }


    /**
     * Return the unique name of the TypeDef.
     *
     * @return String name
     */
    public String getTypeDefName()
    {
        return typeDefName;
    }


    /**
     * Return a patch for the TypeDef.
     *
     * @return TypeDefPatch object
     */
    public TypeDefPatch getTypeDefPatch()
    {
        return typeDefPatch;
    }


    /**
     * Return the details of the TypeDef before it was changed.
     *
     * @return TypeDefSummary containing identifiers, category and version
     */
    public TypeDefSummary getOriginalTypeDefSummary()
    {
        return originalTypeDefSummary;
    }


    /**
     * Return the details of the AttributeTypeDef before it was changed.
     *
     * @return AttributeTypeDef object
     */
    public AttributeTypeDef getOriginalAttributeTypeDef()
    {
        return originalAttributeTypeDef;
    }

    /**
     * Return the TypeDef error code for error events.
     *
     * @return OMRSTypeDefEventErrorCode enum
     */
    public OMRSTypeDefEventErrorCode getErrorCode()
    {
        return errorCode;
    }


    /**
     * Returns an OMRSEvent populated with details from this TypeDefEvent
     *
     * @return OMRSEvent (Version 1) object
     */
    public OMRSEventV1  getOMRSEventV1()
    {
        OMRSEventV1     omrsEvent = super.getOMRSEventV1();

        OMRSEventV1TypeDefSection typeDefSection  = new OMRSEventV1TypeDefSection();

        typeDefSection.setTypeDefEventType(this.typeDefEventType);
        typeDefSection.setTypeDef(this.typeDef);
        typeDefSection.setAttributeTypeDef(this.attributeTypeDef);
        typeDefSection.setTypeDefPatch(this.typeDefPatch);
        typeDefSection.setTypeDefGUID(this.typeDefGUID);
        typeDefSection.setTypeDefName(this.typeDefName);
        typeDefSection.setOriginalTypeDefSummary(this.originalTypeDefSummary);
        typeDefSection.setOriginalAttributeTypeDef(this.originalAttributeTypeDef);

        omrsEvent.setTypeDefEventSection(typeDefSection);

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
        return "OMRSTypeDefEvent{" +
                "typeDefEventType=" + typeDefEventType +
                ", attributeTypeDef=" + attributeTypeDef +
                ", typeDef=" + typeDef +
                ", typeDefGUID='" + typeDefGUID + '\'' +
                ", typeDefName='" + typeDefName + '\'' +
                ", typeDefPatch=" + typeDefPatch +
                ", originalTypeDefSummary=" + originalTypeDefSummary +
                ", originalAttributeTypeDef=" + originalAttributeTypeDef +
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
