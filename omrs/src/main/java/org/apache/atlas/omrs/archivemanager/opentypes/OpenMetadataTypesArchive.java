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

package org.apache.atlas.omrs.archivemanager.opentypes;


import org.apache.atlas.omrs.archivemanager.OMRSArchiveBuilder;
import org.apache.atlas.omrs.archivemanager.OMRSArchiveHelper;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveType;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.ArrayList;
import java.util.Date;

/**
 * OpenMetadataTypesArchive builds an open metadata archive containing all of the standard open metadata types.
 * These types have hardcoded dates and guids so that however many times this archive is rebuilt, it will
 * produce the same content.
 * <p>
 * Details of the open metadata types are documented on the wiki:
 * <a href="https://cwiki.apache.org/confluence/display/ATLAS/Building+out+the+Open+Metadata+Typesystem">Building out the Open Metadata Typesystem</a>
 * </p>
 * <p>
 * There are 8 areas, each covering a different topic area of metadata.  The module breaks down the process of creating
 * the models into the areas and then the individual models to simplify the maintenance of this class
 * </p>
 */
public class OpenMetadataTypesArchive
{
    /*
     * This is the header information for the archive.
     */
    private static final String                  archiveGUID        = "bce3b0a0-662a-4f87-b8dc-844078a11a6e";
    private static final String                  archiveName        = "Open Metadata Types";
    private static final String                  archiveDescription = "Standard types for open metadata repositories.";
    private static final OpenMetadataArchiveType archiveType        = OpenMetadataArchiveType.CONTENT_PACK;
    private static final String                  originatorName     = "Apache Atlas (OMRS)";
    private static final Date                    creationDate       = new Date(1516313040008L);

    /*
     * Specific values for initializing TypeDefs
     */
    private static final long   versionNumber = 1L;
    private static final String versionName   = "1.0";


    private OMRSArchiveBuilder archiveBuilder;
    private OMRSArchiveHelper  archiveHelper;


    /**
     * Default constructor sets up the archive builder.  This in turn sets up the header for the archive.
     */
    public OpenMetadataTypesArchive()
    {
        this.archiveBuilder = new OMRSArchiveBuilder(archiveGUID,
                                                     archiveName,
                                                     archiveDescription,
                                                     archiveType,
                                                     originatorName,
                                                     creationDate,
                                                     null);

        this.archiveHelper = new OMRSArchiveHelper(archiveBuilder,
                                                   archiveGUID,
                                                   originatorName,
                                                   creationDate,
                                                   versionNumber,
                                                   versionName);
    }


    /**
     * Return the unique identifier for this archive.
     *
     * @return String guid
     */
    public String getArchiveGUID()
    {
        return archiveGUID;
    }

    /**
     * Returns the open metadata type archive containing all of the standard open metadata types.
     *
     * @return populated open metadata archive object
     */
    public OpenMetadataArchive getOpenMetadataArchive()
    {
        final String methodName = "getOpenMetadataArchive()";

        if (this.archiveBuilder != null)
        {
            /*
             * Call each of the methods to systematically add the contents of the archive.
             */
            this.addStandardPrimitiveDefs();
            this.addStandardCollectionDefs();
            this.addArea0Types();
            this.addArea1Types();
            this.addArea2Types();
            this.addArea3Types();
            this.addArea4Types();
            this.addArea5Types();
            this.addArea6Types();
            this.addArea7Types();

            /*
             * The completed archive is ready to be packaged up and returned
             */
            return this.archiveBuilder.getOpenMetadataArchive();
        }
        else
        {
            /*
             * This is a logic error since it means the creation of the archive builder threw an exception
             * in the constructor and so this object should not be used.
             */
            OMRSErrorCode errorCode    = OMRSErrorCode.ARCHIVE_UNAVAILABLE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /*
     * ========================================
     * Attribute types
     */


    /**
     * Add the standard primitive types to the archive builder.
     */
    private void addStandardPrimitiveDefs()
    {
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_UNKNOWN));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BOOLEAN));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BYTE));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_CHAR));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_SHORT));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_INT));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_LONG));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_FLOAT));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DOUBLE));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGINTEGER));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGDECIMAL));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING));
        this.archiveBuilder.addPrimitiveDef(archiveHelper.getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DATE));
    }


    /**
     * Add the standard collection types to the archive builder.
     */
    private void addStandardCollectionDefs()
    {
        this.archiveBuilder.addCollectionDef(getMapStringStringCollectionDef());
        this.archiveBuilder.addCollectionDef(getArrayStringCollectionDef());

    }


    /**
     * Defines the "map<string,string>" type.
     *
     * @return CollectionDef for this type
     */
    private CollectionDef getMapStringStringCollectionDef()
    {
        final String guid            = "005c7c14-ac84-4136-beed-959401b041f8";
        final String description     = "A map from String to String.";
        final String descriptionGUID = null;

        return archiveHelper.getMapCollectionDef(guid,
                                                 description,
                                                 descriptionGUID,
                                                 PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING,
                                                 PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
    }


    /**
     * Define the "array<string>" type.
     *
     * @return CollectionDef for this object
     */
    private CollectionDef getArrayStringCollectionDef()
    {
        final String guid            = "0428b5d3-f824-459c-b7f5-f8151de59707";
        final String description     = "An array of Strings.";
        final String descriptionGUID = null;

        return archiveHelper.getArrayCollectionDef(guid,
                                                   description,
                                                   descriptionGUID,
                                                   PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);
    }




    /*
     * ========================================
     * AREA 0 - common types and infrastructure
     */

    /**
     * Add the list of area 0 types
     */
    private void addArea0Types()
    {
        this.add0010BaseModel();
        this.add0015LinkedMediaTypes();
        this.add0017ExternalIdentifiers();
        this.add0020PropertyFacets();
        this.add0025Locations();
        this.add0030HostsAndPlatforms();
        this.add0035ComplexHosts();
        this.add0040SoftwareServers();
        this.add0045ServersAndAssets();
        this.add0070NetworksAndGateways();
        this.add0090CloudPlatformsAndServices();
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0010 Base Model defines the core entities that have been inherited from the original Apache Atlas model.
     * It defines an initial set of asset types that need to be governed (more assets are defined in Area 2).
     */
    private void add0010BaseModel()
    {
        this.archiveBuilder.addEntityDef(getReferenceableEntity());
        this.archiveBuilder.addEntityDef(getAssetEntity());
        this.archiveBuilder.addEntityDef(getInfrastructureEntity());
        this.archiveBuilder.addEntityDef(getProcessEntity());
        this.archiveBuilder.addEntityDef(getDataSetEntity());

        this.archiveBuilder.addRelationshipDef(getProcessInputRelationship());
        this.archiveBuilder.addRelationshipDef(getProcessOutputRelationship());
    }


    /**
     * The Referenceable entity is the superclass of all of the governed open metadata entities.  It specifies that
     * these entities have a unique identifier called "qualifiedName".
     *
     * @return Referenceable EntityDef
     */
    private EntityDef getReferenceableEntity()
    {
        /*
         * Build the Entity
         */
        final String guid            = "a32316b8-dc8c-48c5-b12b-71c1b2a080bf";
        final String name            = "Referenceable";
        final String description     = "An open metadata entity that has a unique identifier.";
        final String descriptionGUID = null;

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                null,
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "qualifiedName";
        final String attribute1Description     = "Unique identifier for the entity.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "additionalProperties";
        final String attribute2Description     = "Additional properties for the element.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        property.setUnique(true);
        properties.add(property);
        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    /**
     * The Asset entity is the root entity for the assets that open metadata and governance is governing.
     *
     * @return Asset EntityDef
     */
    private EntityDef getAssetEntity()
    {
        /*
         * Build the Entity
         */
        final String guid            = "896d14c2-7522-4f6c-8519-757711943fe6";
        final String name            = "Asset";
        final String description     = "The description of an asset that needs to be catalogued and governed.";
        final String descriptionGUID = null;
        final String superTypeName   = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Display name for the asset.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the asset.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "owner";
        final String attribute3Description     = "User name of the person or process that owns the asset.";
        final String attribute3DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    /**
     * The Infrastructure entity describes an asset that is physical infrastructure or part of the software
     * platform that supports the data and process assets.
     *
     * @return Infrastructure EntityDef
     */
    private EntityDef getInfrastructureEntity()
    {
        /*
         * Build the Entity
         */
        final String guid            = "c19746ac-b3ec-49ce-af4b-83348fc55e07";
        final String name            = "Infrastructure";
        final String description     = "Physical infrastructure or software platform.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /**
     * The Process entity describes a well-defined sequence of activities performed by people or software components.
     *
     * @return Process EntityDef
     */
    private EntityDef getProcessEntity()
    {
        /*
         * Build the Entity
         */
        final String guid            = "d8f33bd7-afa9-4a11-a8c7-07dcec83c050";
        final String name            = "Process";
        final String description     = "Well-defined sequence of activities performed by people or software components.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /**
     * The DataSet entity describes a collection of related data.
     *
     * @return DataSet EntityDef
     */
    private EntityDef getDataSetEntity()
    {
        /*
         * Build the Entity
         */
        final String guid            = "1449911c-4f44-4c22-abc0-7540154feefb";
        final String name            = "DataSet";
        final String description     = "Collection of related data.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /**
     * The ProcessInput relationship describes the data set(s) that are passed into a process.
     *
     * @return ProcessInput RelationshipDef
     */
    private RelationshipDef getProcessInputRelationship()
    {
        /*
         * Build the relationship
         */
        final String guid            = "9a6583c4-7419-4d5a-a6e5-26b0033fa349";
        final String name            = "ProcessInput";
        final String description     = "The DataSets passed into a Process.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Process";
        final String               end1AttributeName            = "consumedByProcess";
        final String               end1AttributeDescription     = "Processes that consume this DataSet.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataSet";
        final String               end2AttributeName            = "processInputData";
        final String               end2AttributeDescription     = "DataSets consumed by this Process.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    /**
     * The ProcessOutput relationship describes the data set(s) that are produced by a process.
     *
     * @return ProcessOutput RelationshipDef
     */
    private RelationshipDef getProcessOutputRelationship()
    {
        /*
         * Build the relationship
         */
        final String                        guid                          = "8920eada-9b05-4368-b511-b8506a4bef4b";
        final String                        name                          = "ProcessOutput";
        final String                        description                   = "The DataSets produced by a Process.";
        final String                        descriptionGUID               = null;
        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Process";
        final String               end1AttributeName            = "producedByProcess";
        final String               end1AttributeDescription     = "Processes that produce this DataSet.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataSet";
        final String               end2AttributeName            = "processOutputData";
        final String               end2AttributeDescription     = "DataSets produced by this Process.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0015 Linked Media Types describe different media (like images and documents) that enhance the description
     * of an entity.  Media entities can be added to any Referenceable entities.
     */
    private void add0015LinkedMediaTypes()
    {
        this.archiveBuilder.addEnumDef(getMediaUsageTypeEnum());

        this.archiveBuilder.addEntityDef(getExternalReferenceEntity());
        this.archiveBuilder.addEntityDef(getRelatedMediaEntity());
        this.archiveBuilder.addEntityDef(getMediaUsageEntity());


        this.archiveBuilder.addRelationshipDef(getExternalReferenceLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getMediaReferenceRelationship());
        this.archiveBuilder.addRelationshipDef(getMediaUsageGuidanceRelationship());

    }


    private EnumDef getMediaUsageTypeEnum()
    {
        final String guid            = "c6861a72-7485-48c9-8040-876f6c342b61";
        final String name            = "MediaUsageType";
        final String description     = "Defines how a related media reference should be used.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Icon";
        final String element1Description     = "Provides a small image to represent the asset in tree views and graphs.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Thumbnail";
        final String element2Description     = "Provides a small image about the asset that can be used in lists.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Illustration";
        final String element3Description     = "Illustrates how the asset works or what it contains. It is complementary to the asset's description.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "UsageGuidance";
        final String element4Description     = "Provides guidance to a person on how to use the asset.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 99;
        final String element5Value           = "Other";
        final String element5Description     = "Another usage.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getExternalReferenceEntity()
    {
        final String guid            = "af536f20-062b-48ef-9c31-1ddd05b04c56";
        final String name            = "ExternalReference";
        final String description     = "A link to more information.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "url";
        final String attribute2Description     = "Location of the external reference.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "version";
        final String attribute3Description     = "Version number of the external reference.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "description";
        final String attribute4Description     = "Description of the external reference.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getRelatedMediaEntity()
    {
        final String guid            = "747f8b86-fe7c-4c9b-ba75-979e093cc307";
        final String name            = "RelatedMedia";
        final String description     = "Images, video or sound media.";
        final String descriptionGUID = null;

        final String superTypeName = "ExternalReference";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getMediaUsageEntity()
    {
        final String guid            = "b9599da3-ce7e-4981-b25e-86d03340da0b";
        final String name            = "MediaUsage";
        final String description     = "Guidance on a particular way a specific piece of media could be used.";
        final String descriptionGUID = null;

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                null,
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "mediaUsageType";
        final String attribute1Description     = "Type of media usage.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "guidance";
        final String attribute2Description     = "Advice on how the media should be used in this context.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("MediaUsageType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getExternalReferenceLinkRelationship()
    {
        final String guid            = "7d818a67-ab45-481c-bc28-f6b1caf12f06";
        final String name            = "ExternalReferenceLink";
        final String description     = "Link to more information.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "relatedItem";
        final String               end1AttributeDescription     = "Item that is referencing this work.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalReference";
        final String               end2AttributeName            = "externalReference";
        final String               end2AttributeDescription     = "Link to more information from an external source.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "referenceId";
        final String attribute1Description     = "Local identifier for the reference.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the relevance of this reference to the linked item.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getMediaReferenceRelationship()
    {
        final String guid            = "1353400f-b0ab-4ab9-ab09-3045dd8a7140";
        final String name            = "MediaReference";
        final String description     = "Link to related media such as images, videos and audio.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "consumingItem";
        final String               end1AttributeDescription     = "Item that is referencing this work.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "RelatedMedia";
        final String               end2AttributeName            = "relatedMedia";
        final String               end2AttributeDescription     = "Link to external media.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "mediaId";
        final String attribute1Description     = "Local identifier for the media.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the relevance of this media to the linked item.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getMediaUsageGuidanceRelationship()
    {
        final String guid            = "952b5c8b-42fb-4ff2-a15f-a9eebb82d2ca";
        final String name            = "MediaUsageGuidance";
        final String description     = "Link to information on how a specific piece of media could be used.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "RelatedMedia";
        final String               end1AttributeName            = "parentMedia";
        final String               end1AttributeDescription     = "Parent media that this guidance is for.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "MediaUsage";
        final String               end2AttributeName            = "usageRecommendation";
        final String               end2AttributeDescription     = "Link to guidance on how related media should be used.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0017 External Identifiers define identifiers used to identify this entity in other systems.
     */
    private void add0017ExternalIdentifiers()
    {
        this.archiveBuilder.addEnumDef(getKeyPatternEnum());

        this.archiveBuilder.addEntityDef(getExternalIdEntity());

        this.archiveBuilder.addRelationshipDef(getExternalIdScopeRelationship());
        this.archiveBuilder.addRelationshipDef(getExternalIdLinkRelationship());
    }


    private EnumDef getKeyPatternEnum()
    {
        final String guid            = "8904df8f-1aca-4de8-9abd-1ef2aadba300";
        final String name            = "KeyPattern";
        final String description     = "Defines the type of identifier used for an asset.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "LocalKey";
        final String element1Description     = "Unique key allocated and used within the scope of a single system.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "RecycledKey";
        final String element2Description     = "Key allocated and used within the scope of a single system that is periodically reused for different records.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "NaturalKey";
        final String element3Description     = "Key derived from an attribute of the entity, such as email address, passport number.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "MirrorKey";
        final String element4Description     = "Key value copied from another system.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "AggregateKey";
        final String element5Description     = "Key formed by combining keys from multiple systems.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "CallersKey";
        final String element6Description     = "Key from another system can bey used if system name provided.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element7Ordinal         = 6;
        final String element7Value           = "StableKey";
        final String element7Description     = "Key value will remain active even if records are merged.";
        final String element7DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element7Ordinal, element7Value, element7Description, element7DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element8Ordinal         = 99;
        final String element8Value           = "Other";
        final String element8Description     = "Another key pattern.";
        final String element8DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element8Ordinal, element8Value, element8Description, element8DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getExternalIdEntity()
    {
        final String guid            = "7c8f8c2c-cc48-429e-8a21-a1f1851ccdb0";
        final String name            = "ExternalId";
        final String description     = "Alternative identifier used in another system.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "identifier";
        final String attribute1Description     = "Identifier used in an external system.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "keyPattern";
        final String attribute2Description     = "Management pattern associated with the identifier.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("KeyPattern", attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getExternalIdScopeRelationship()
    {
        final String guid            = "8c5b1415-2d1f-4190-ba6c-1fdd47f03269";
        final String name            = "ExternalIdScope";
        final String description     = "Places where an external identifier is recognized.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "scopedTo";
        final String               end1AttributeDescription     = "Identifies where this external identifier is known.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalId";
        final String               end2AttributeName            = "managedResources";
        final String               end2AttributeDescription     = "Link to details of a resource that this component manages.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship between the resources and the managing component.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);


        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getExternalIdLinkRelationship()
    {
        final String guid            = "28ab0381-c662-4b6d-b787-5d77208de126";
        final String name            = "ExternalIdLink";
        final String description     = "Link between an external identifier and an asset or related item.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "resource";
        final String               end1AttributeDescription     = "Resource being identified.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalId";
        final String               end2AttributeName            = "alsoKnownAs";
        final String               end2AttributeDescription     = "Identifier used in an external system.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of how the external identifier relates to the resource.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "usage";
        final String attribute2Description     = "Description of how the external identifier can be used.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "source";
        final String attribute3Description     = "Details of where the external identifier was sourced from.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0020 Property Facets define blocks of properties that are unique to a particular vendor or service.
     */
    private void add0020PropertyFacets()
    {
        this.archiveBuilder.addEntityDef(getPropertyFacetEntity());

        this.archiveBuilder.addRelationshipDef(getReferenceableFacetRelationship());
    }


    private EntityDef getPropertyFacetEntity()
    {
        final String guid            = "6403a704-aad6-41c2-8e08-b9525c006f85";
        final String name            = "PropertyFacet";
        final String description     = "Additional properties that support a particular vendor or service.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "version";
        final String attribute1Description     = "Version of the property facet schema.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the property facet contents.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "properties";
        final String attribute3Description     = "Properties for the property facet.";
        final String attribute3DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getReferenceableFacetRelationship()
    {
        final String guid            = "58c87647-ada9-4c90-a3c3-a40ace46b1f7";
        final String name            = "ReferenceableFacet";
        final String description     = "Link between a property facet and the resource it relates to.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "relatedEntity";
        final String               end1AttributeDescription     = "Identifies which element this property facet belongs to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "PropertyFacet";
        final String               end2AttributeName            = "facets";
        final String               end2AttributeDescription     = "Additional properties from different sources.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "source";
        final String attribute1Description     = "Source of this property facet.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0025 Locations define entities that describe physical, logical and cyber locations for Assets.
     */
    private void add0025Locations()
    {
        this.archiveBuilder.addEntityDef(getLocationEntity());

        this.archiveBuilder.addRelationshipDef(getNestedLocationRelationship());
        this.archiveBuilder.addRelationshipDef(getAdjacentLocationRelationship());
        this.archiveBuilder.addRelationshipDef(getAssetLocationRelationship());

        this.archiveBuilder.addClassificationDef(getMobileAssetClassification());
        this.archiveBuilder.addClassificationDef(getFixedLocationClassification());
        this.archiveBuilder.addClassificationDef(getSecureLocationClassification());
        this.archiveBuilder.addClassificationDef(getCyberLocationClassification());
    }


    private EntityDef getLocationEntity()
    {
        final String guid            = "3e09cb2b-5f15-4fd2-b004-fe0146ad8628";
        final String name            = "Location";
        final String description     = "A physical place, digital location or area.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the location.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getNestedLocationRelationship()
    {
        final String guid            = "f82a96c2-95a3-4223-88c0-9cbf2882b772";
        final String name            = "NestedLocation";
        final String description     = "Link between two locations to show one is nested inside another.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Location";
        final String               end1AttributeName            = "groupingLocation";
        final String               end1AttributeDescription     = "Location that is covering the broader area.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Location";
        final String               end2AttributeName            = "nestedLocation";
        final String               end2AttributeDescription     = "Location that is nested in this location.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAdjacentLocationRelationship()
    {
        final String guid            = "017d0518-fc25-4e5e-985e-491d91e61e17";
        final String name            = "AdjacentLocation";
        final String description     = "Link between two locations that are next to one another.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Location";
        final String               end1AttributeName            = "peerLocation";
        final String               end1AttributeDescription     = "Location that is adjacent to this location.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Location";
        final String               end2AttributeName            = "peerLocation";
        final String               end2AttributeDescription     = "Location that is adjacent to this location.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAssetLocationRelationship()
    {
        final String guid            = "bc236b62-d0e6-4c5c-93a1-3a35c3dba7b1";
        final String name            = "AssetLocation";
        final String description     = "Location of an Asset.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Location";
        final String               end1AttributeName            = "knownLocations";
        final String               end1AttributeDescription     = "Places where this asset is sited.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Location";
        final String               end2AttributeName            = "localAssets";
        final String               end2AttributeDescription     = "Assets sited at this location.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getMobileAssetClassification()
    {
        final String guid            = "b25fb90d-8fa2-4aa9-b884-ff0a6351a697";
        final String name            = "MobileAsset";
        final String description     = "An asset not restricted to a single physical location.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Asset";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getFixedLocationClassification()
    {
        final String guid            = "bc111963-80c7-444f-9715-946c03142dd2";
        final String name            = "FixedLocation";
        final String description     = "A location linked to a physical place.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Location";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "coordinates";
        final String attribute1Description     = "Geographical coordinates of this location.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "address";
        final String attribute2Description     = "Postal address of this location.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "timezone";
        final String attribute3Description     = "Timezone for the location.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getSecureLocationClassification()
    {
        final String guid            = "e7b563c0-fcdd-4ba7-a046-eecf5c4638b8";
        final String name            = "SecureLocation";
        final String description     = "A location that protects the assets in its care.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Location";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the security at this location.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "level";
        final String attribute2Description     = "Level of security at this location.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCyberLocationClassification()
    {
        final String guid            = "f9ec3633-8ac8-480b-aa6d-5e674b9e1b17";
        final String name            = "CyberLocation";
        final String description     = "A digital location.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Location";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "address";
        final String attribute1Description     = "Network address of the location.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0030 Hosts and Platforms describe the Software and Hardware platforms hosting Assets.
     */
    private void add0030HostsAndPlatforms()
    {
        this.archiveBuilder.addEnumDef(getEndiannessEnum());

        this.archiveBuilder.addEntityDef(getITInfrastructureEntity());
        this.archiveBuilder.addEntityDef(getHostEntity());
        this.archiveBuilder.addEntityDef(getOperatingPlatformEntity());

        this.archiveBuilder.addRelationshipDef(getHostLocationRelationship());
        this.archiveBuilder.addRelationshipDef(getHostOperatingPlatformRelationship());
    }


    private EnumDef getEndiannessEnum()
    {
        final String guid            = "e5612c3a-49bd-4148-8f67-cfdf145d5fd8";
        final String name            = "Endianness";
        final String description     = "Defines the sequential order in which bytes are arranged into larger numerical values when stored in memory or when transmitted over digital links.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "BigEndian";
        final String element1Description     = "Bits or bytes order from the big end.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "LittleEndian";
        final String element2Description     = "Bits or bytes ordered from the little end.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getITInfrastructureEntity()
    {
        final String guid            = "151e6dd1-54a0-4b7f-a072-85caa09d1dda";
        final String name            = "ITInfrastructure";
        final String description     = "Hardware and base software that supports an IT system.";
        final String descriptionGUID = null;

        final String superTypeName = "Infrastructure";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getHostEntity()
    {
        final String guid            = "1abd16db-5b8a-4fd9-aee5-205db3febe99";
        final String name            = "Host";
        final String description     = "Named IT infrastructure system that supports multiple software servers.";
        final String descriptionGUID = null;

        final String superTypeName = "ITInfrastructure";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getOperatingPlatformEntity()
    {
        final String guid            = "bd96a997-8d78-42f6-adf7-8239bc98501c";
        final String name            = "OperatingPlatform";
        final String description     = "Characteristics of the operating system in use within a host.";
        final String descriptionGUID = null;

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                null,
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the operating platform.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the operating platform.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "operatingSystem";
        final String attribute3Description     = "Name of the operating system running on this operating platform.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "endianness";
        final String attribute4Description     = "Definition of byte ordering.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getHostLocationRelationship()
    {
        final String guid            = "f3066075-9611-4886-9244-32cc6eb07ea9";
        final String name            = "HostLocation";
        final String description     = "Defines the location of a host.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Host";
        final String               end1AttributeName            = "host";
        final String               end1AttributeDescription     = "Host sited at this location.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Location";
        final String               end2AttributeName            = "locations";
        final String               end2AttributeDescription     = "Locations for this host.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getHostOperatingPlatformRelationship()
    {
        final String guid            = "b9179df5-6e23-4581-a8b0-2919e6322b12";
        final String name            = "HostOperatingPlatform";
        final String description     = "Identifies the operating platform for a host.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Host";
        final String               end1AttributeName            = "host";
        final String               end1AttributeDescription     = "Host supporting this operating platform.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "OperatingPlatform";
        final String               end2AttributeName            = "platform";
        final String               end2AttributeDescription     = "Type of platform supported by this host.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0035 Complex Hosts describe virtualization and clustering options for Hosts.
     */
    private void add0035ComplexHosts()
    {
        this.archiveBuilder.addEntityDef(getHostClusterEntity());
        this.archiveBuilder.addEntityDef(getVirtualContainerEntity());

        this.archiveBuilder.addRelationshipDef(getHostClusterMemberRelationship());
        this.archiveBuilder.addRelationshipDef(getDeployedVirtualContainerRelationship());
    }


    private EntityDef getHostClusterEntity()
    {
        final String guid            = "9794f42f-4c9f-4fe6-be84-261f0a7de890";
        final String name            = "HostCluster";
        final String description     = "A group of hosts operating together to provide a scalable platform.";
        final String descriptionGUID = null;

        final String superTypeName = "Host";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getVirtualContainerEntity()
    {
        final String guid            = "e2393236-100f-4ac0-a5e6-ce4e96c521e7";
        final String name            = "VirtualContainer";
        final String description     = "Container-based virtual host.";
        final String descriptionGUID = null;

        final String superTypeName = "Host";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getHostClusterMemberRelationship()
    {
        final String guid            = "1a1c3933-a583-4b0c-9e42-c3691296a8e0";
        final String name            = "HostClusterMember";
        final String description     = "Identifies a host as a member of a host cluster.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "HostCluster";
        final String               end1AttributeName            = "hostCluster";
        final String               end1AttributeDescription     = "Cluster managing this host.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Host";
        final String               end2AttributeName            = "hosts";
        final String               end2AttributeDescription     = "Member of the host cluster.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    private RelationshipDef getDeployedVirtualContainerRelationship()
    {
        final String guid            = "4b981d89-e356-4d9b-8f17-b3a8d5a86676";
        final String name            = "DeployedVirtualContainer";
        final String description     = "Identifies the real host where a virtual container is deployed to.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Host";
        final String               end1AttributeName            = "hosts";
        final String               end1AttributeDescription     = "Deployed host for this container.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "VirtualContainer";
        final String               end2AttributeName            = "hostedContainers";
        final String               end2AttributeDescription     = "Virtual containers deployed on this host.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0040 Software Servers describe the structure of a software server and its capabilities.
     */
    private void add0040SoftwareServers()
    {
        this.archiveBuilder.addEnumDef(getOperationalStatusEnum());

        this.archiveBuilder.addEntityDef(getSoftwareServerEntity());
        this.archiveBuilder.addEntityDef(getEndpointEntity());
        this.archiveBuilder.addEntityDef(getSoftwareServerCapabilityEntity());

        this.archiveBuilder.addRelationshipDef(getServerDeploymentRelationship());
        this.archiveBuilder.addRelationshipDef(getServerSupportedCapabilityRelationship());
        this.archiveBuilder.addRelationshipDef(getServerEndpointRelationship());
    }


    private EnumDef getOperationalStatusEnum()
    {
        final String guid            = "24e1e33e-9250-4a6c-8b07-05c7adec3a1d";
        final String name            = "OperationalStatus";
        final String description     = "Defines whether a component is operational.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Disabled";
        final String element1Description     = "The component is not operational.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Enabled";
        final String element2Description     = "The component is operational.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getSoftwareServerEntity()
    {
        final String guid            = "aa7c7884-32ce-4991-9c41-9778f1fec6aa";
        final String name            = "SoftwareServer";
        final String description     = "Software services to support a runtime environment for applications and data stores.";
        final String descriptionGUID = null;

        final String superTypeName = "ITInfrastructure";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of software server.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "version";
        final String attribute2Description     = "Version number of the software server.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "source";
        final String attribute3Description     = "Supplier of the software server.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "userId";
        final String attribute4Description     = "Server's authentication name.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getEndpointEntity()
    {
        final String guid            = "dbc20663-d705-4ff0-8424-80c262c6b8e7";
        final String name            = "Endpoint";
        final String description     = "Description of the network address and related information needed to call a software service.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the endpoint.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the endpoint and its capabilities.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "networkAddress";
        final String attribute3Description     = "Name used to connect to the endpoint.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "protocol";
        final String attribute4Description     = "Name of the protocol used to connect to the endpoint.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "encryptionMethod";
        final String attribute5Description     = "Type of encryption used at the endpoint (if any).";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getSoftwareServerCapabilityEntity()
    {
        final String guid            = "fe30a033-8f86-4d17-8986-e6166fa24177";
        final String name            = "SoftwareServerCapability";
        final String description     = "A software capability such as an application, that is deployed to a software server.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the software server capability.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the software server capability.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "type";
        final String attribute3Description     = "Type of the software server capability.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "version";
        final String attribute4Description     = "Version number of the software server capability.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "patchLevel";
        final String attribute5Description     = "Patch level of the software server capability.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "source";
        final String attribute6Description     = "Supplier of the software server capability.";
        final String attribute6DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getServerDeploymentRelationship()
    {
        final String guid            = "d909eb3b-5205-4180-9f63-122a65b30738";
        final String name            = "ServerDeployment";
        final String description     = "Defines the host that a software server is deployed to.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Host";
        final String               end1AttributeName            = "host";
        final String               end1AttributeDescription     = "Supporting host.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SoftwareServer";
        final String               end2AttributeName            = "deployedServers";
        final String               end2AttributeDescription     = "Software servers deployed on this host.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "deploymentTime";
        final String attribute1Description     = "Time that the software server was deployed to the host.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "deployer";
        final String attribute2Description     = "Person, organization or engine that deployed the software server.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "serverCapabilityStatus";
        final String attribute3Description     = "The operational status of the software server on this host.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("OperationalStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getServerSupportedCapabilityRelationship()
    {
        final String guid            = "8b7d7da5-0668-4174-a43b-8f8c6c068dd0";
        final String name            = "ServerSupportedCapability";
        final String description     = "Identifies a software capability that is deployed to a software server.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SoftwareServer";
        final String               end1AttributeName            = "servers";
        final String               end1AttributeDescription     = "Hosting server.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SoftwareServerCapability";
        final String               end2AttributeName            = "capabilities";
        final String               end2AttributeDescription     = "Software servers deployed on this host.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "deploymentTime";
        final String attribute1Description     = "Time that the software server capability was deployed to the software server.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "deployer";
        final String attribute2Description     = "Person, organization or engine that deployed the software server capability.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "serverCapabilityStatus";
        final String attribute3Description     = "The operational status of the software server capability on this software server.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("OperationalStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getServerEndpointRelationship()
    {
        final String guid            = "2b8bfab4-8023-4611-9833-82a0dc95f187";
        final String name            = "ServerEndpoint";
        final String description     = "Defines an endpoint associated with a server.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SoftwareServer";
        final String               end1AttributeName            = "servers";
        final String               end1AttributeDescription     = "Server supporting this endpoint.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Endpoint";
        final String               end2AttributeName            = "endpoints";
        final String               end2AttributeDescription     = "Endpoints supported by this server.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0045 Servers and Assets defines the relationships between SoftwareServers and Assets.
     */
    private void add0045ServersAndAssets()
    {
        this.archiveBuilder.addEnumDef(getServerAssetUseTypeEnum());

        this.archiveBuilder.addRelationshipDef(getServerAssetUseRelationship());
    }


    private EnumDef getServerAssetUseTypeEnum()
    {
        final String guid            = "09439481-9489-467c-9ae5-178a6e0b6b5a";
        final String name            = "ServerAssetUse";
        final String description     = "Defines how a server capability may use an asset.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Owns";
        final String element1Description     = "The server capability is accountable for the maintenance and protection of the asset.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Governs";
        final String element2Description     = "The server capability provides management or oversight of the asset.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Maintains";
        final String element3Description     = "The server capability keeps the asset up-to-date.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Uses";
        final String element4Description     = "The server capability consumes the content of the asset.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 99;
        final String element5Value           = "Other";
        final String element5Description     = "Another usage.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private RelationshipDef getServerAssetUseRelationship()
    {
        final String guid            = "92b75926-8e9a-46c7-9d98-89009f622397";
        final String name            = "AssetServerUse";
        final String description     = "Defines that a server capability is using an asset.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SoftwareServerCapability";
        final String               end1AttributeName            = "consumedBy";
        final String               end1AttributeDescription     = "Capability consuming this asset.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Asset";
        final String               end2AttributeName            = "consumedAsset";
        final String               end2AttributeDescription     = "Asset that this software server capability is dependent on.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "useType";
        final String attribute1Description     = "Describes how the software server capability uses the asset.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Additional information on how the asset is use by the software server capability.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("ServerAssetUse", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0070 Networks and Gateways provide a very simple network model.
     */
    private void add0070NetworksAndGateways()
    {
        this.archiveBuilder.addEntityDef(getNetworkEntity());
        this.archiveBuilder.addEntityDef(getNetworkGatewayEntity());

        this.archiveBuilder.addRelationshipDef(getHostNetworkRelationship());
        this.archiveBuilder.addRelationshipDef(getNetworkGatewayLinkRelationship());
    }


    private EntityDef getNetworkEntity()
    {
        final String guid            = "e0430f59-f021-411a-9d81-883e1ff3f6f6";
        final String name            = "Network";
        final String description     = "Inter-connectivity for systems.";
        final String descriptionGUID = null;

        final String superTypeName = "ITInfrastructure";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getNetworkGatewayEntity()
    {
        final String guid            = "9bbae94d-e109-4c96-b072-4f97123f04fd";
        final String name            = "NetworkGateway";
        final String description     = "A connection point enabling network traffic to pass between two networks.";
        final String descriptionGUID = null;

        final String superTypeName = "SoftwareServerCapability";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getHostNetworkRelationship()
    {
        final String guid            = "f2bd7401-c064-41ac-862c-e5bcdc98fa1e";
        final String name            = "HostNetwork";
        final String description     = "One of the hosts connected to a network.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Host";
        final String               end1AttributeName            = "connectedHosts";
        final String               end1AttributeDescription     = "Hosts connected to this network.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Network";
        final String               end2AttributeName            = "networkConnections";
        final String               end2AttributeDescription     = "Connections to different networks.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getNetworkGatewayLinkRelationship()
    {
        final String guid            = "5bece460-1fa6-41fb-a29f-fdaf65ec8ce3";
        final String name            = "NetworkGatewayLink";
        final String description     = "Link from a network to one of its network gateways.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "NetworkGateway";
        final String               end1AttributeName            = "gateways";
        final String               end1AttributeDescription     = "Gateways to other networks.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Network";
        final String               end2AttributeName            = "networkConnections";
        final String               end2AttributeDescription     = "Connections to different networks.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0090 Cloud Platforms and Services provides classifications for infrastructure to allow cloud platforms
     * and services to be identified.
     */
    private void add0090CloudPlatformsAndServices()
    {
        this.archiveBuilder.addClassificationDef(getCloudProviderClassification());
        this.archiveBuilder.addClassificationDef(getCloudPlatformClassification());
        this.archiveBuilder.addClassificationDef(getCloudServiceClassification());
    }


    private ClassificationDef getCloudProviderClassification()
    {
        final String guid            = "a2bfdd08-d0a8-49db-bc97-7f2406281046";
        final String name            = "CloudProvider";
        final String description     = "A host supporting cloud services.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Host";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "providerName";
        final String attribute1Description     = "Name of the cloud provider.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCloudPlatformClassification()
    {
        final String guid            = "1b8f8511-e606-4f65-86d3-84891706ad12";
        final String name            = "CloudPlatform";
        final String description     = "A server supporting cloud services.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of cloud platform.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCloudServiceClassification()
    {
        final String guid            = "337e7b1a-ad4b-4818-aa3e-0ff3307b2fbe";
        final String name            = "CloudService";
        final String description     = "A service running on a cloud platform.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServerCapability";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "offeringName";
        final String attribute1Description     = "Commercial name of the service.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "type";
        final String attribute2Description     = "Description of the type of the service.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }

    /*
     * ========================================
     * AREA 1 - collaboration
     */

    private void addArea1Types()
    {
        this.add0110Actors();
        this.add0120Collections();
        this.add0130Projects();
        this.add0135Meetings();
        this.add0140Communities();
        this.add0150Feedback();
        this.add0160Notes();
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0110 Actors describe the people and their relationships who are using the Assets.
     */
    private void add0110Actors()
    {
        this.archiveBuilder.addEnumDef(getCrowdSourcingRoleEnum());
        this.archiveBuilder.addEnumDef(getContactMethodTypeEnum());

        this.archiveBuilder.addEntityDef(getActorProfileEntity());
        this.archiveBuilder.addEntityDef(getTeamEntity());
        this.archiveBuilder.addEntityDef(getPersonEntity());
        this.archiveBuilder.addEntityDef(getUserIdentityEntity());
        this.archiveBuilder.addEntityDef(getContactDetailsEntity());

        this.archiveBuilder.addRelationshipDef(getContactThroughRelationship());
        this.archiveBuilder.addRelationshipDef(getLeadershipRelationship());
        this.archiveBuilder.addRelationshipDef(getPeerRelationship());
        this.archiveBuilder.addRelationshipDef(getProfileIdentityRelationship());
        this.archiveBuilder.addRelationshipDef(getContributorRelationship());
    }


    private EnumDef getCrowdSourcingRoleEnum()
    {
        final String guid            = "0ded50c2-17cc-4ecf-915e-908e66dbb27f";
        final String name            = "CrowdSourcingRole";
        final String description     = "Type of contributor to new information and/or assets.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Proposer";
        final String element1Description     = "Actor that creates the initial version.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Reviewer";
        final String element2Description     = "Actor that provided feedback.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Supporter";
        final String element3Description     = "Actor that agrees with the definition.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Approver";
        final String element4Description     = "Actor that declares the definition should be used.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 99;
        final String element5Value           = "Other";
        final String element5Description     = "Another role.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getContactMethodTypeEnum()
    {
        final String guid            = "30e7d8cd-df01-46e8-9247-a24c5650910d";
        final String name            = "ContactMethodType";
        final String description     = "Mechanism to contact an individual.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Email";
        final String element1Description     = "Contact through email.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Phone";
        final String element2Description     = "Contact through telephone number.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Account";
        final String element3Description     = "Contact through social media or similar account.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 99;
        final String element5Value           = "Other";
        final String element5Description     = "Another usage.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getActorProfileEntity()
    {
        final String guid            = "5a2f38dc-d69d-4a6f-ad26-ac86f118fa35";
        final String name            = "ActorProfile";
        final String description     = "Description of a person, team or automated process that is working with data.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the actor.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the actor.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getTeamEntity()
    {
        final String guid            = "36db26d5-aba2-439b-bc15-d62d373c5db6";
        final String name            = "Team";
        final String description     = "Group of people working together.";
        final String descriptionGUID = null;

        final String superTypeName = "ActorProfile";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getPersonEntity()
    {
        final String guid            = "ac406bf8-e53e-49f1-9088-2af28bbbd285";
        final String name            = "Person";
        final String description     = "An individual.";
        final String descriptionGUID = null;

        final String superTypeName = "ActorProfile";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "karmaPoints";
        final String attribute1Description     = "Points capturing a person's engagement with open metadata.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getIntTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getUserIdentityEntity()
    {
        final String guid            = "fbe95779-1f3c-4ac6-aa9d-24963ff16282";
        final String name            = "UserIdentity";
        final String description     = "Name of the security account for a person or automated process.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "userId";
        final String attribute1Description     = "Authentication name.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getContactDetailsEntity()
    {
        final String guid            = "79296df8-645a-4ef7-a011-912d1cdcf75a";
        final String name            = "ContactDetails";
        final String description     = "Information on how to send a message to an individual or automated process.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "contactMethodType";
        final String attribute1Description     = "Method to contact an actor.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "contactMethodValue";
        final String attribute2Description     = "Details of the contact method.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("ContactMethodType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getContactThroughRelationship()
    {
        final String guid            = "6cb9af43-184e-4dfa-854a-1572bcf0fe75";
        final String name            = "ContactThrough";
        final String description     = "The contact details associated with an actor profile.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ActorProfile";
        final String               end1AttributeName            = "contactDetails";
        final String               end1AttributeDescription     = "Contact details owner.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ContactDetails";
        final String               end2AttributeName            = "contacts";
        final String               end2AttributeDescription     = "Contact information.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "contactMethodType";
        final String attribute1Description     = "Mechanism to use.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "contactMethodValue";
        final String attribute2Description     = "Contact address.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("ContactMethodType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getLeadershipRelationship()
    {
        final String guid            = "5ebc4fb2-b62a-4269-8f18-e9237a2119ca";
        final String name            = "Leadership";
        final String description     = "Relationship identifying leaders and their followers.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ActorProfile";
        final String               end1AttributeName            = "leads";
        final String               end1AttributeDescription     = "The leader.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ActorProfile";
        final String               end2AttributeName            = "follows";
        final String               end2AttributeDescription     = "Follower.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getPeerRelationship()
    {
        final String guid            = "4a316abe-bccd-4d11-ad5a-4bfb4079b80b";
        final String name            = "Peer";
        final String description     = "Relationship identifying actor profile who are peers.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ActorProfile";
        final String               end1AttributeName            = "peers";
        final String               end1AttributeDescription     = "Peers.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ActorProfile";
        final String               end2AttributeName            = "peers";
        final String               end2AttributeDescription     = "Peers.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getProfileIdentityRelationship()
    {
        final String guid            = "01664609-e777-4079-b543-6baffe910ff1";
        final String name            = "ProfileIdentity";
        final String description     = "Correlates a user identity with an actor profile.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ActorProfile";
        final String               end1AttributeName            = "profile";
        final String               end1AttributeDescription     = "Description of the person, organization or engine that uses this user identity.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "UserIdentity";
        final String               end2AttributeName            = "userIdentities";
        final String               end2AttributeDescription     = "Authentication identifiers in use by the owner of this profile.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "userId";
        final String attribute1Description     = "Authentication name used by the user.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getContributorRelationship()
    {
        final String guid            = "4db83564-b200-4956-94a4-c95a5c30e65a";
        final String name            = "Contributor";
        final String description     = "Defines one of the actors contributing content to a new description or asset.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "contributions";
        final String               end1AttributeDescription     = "Items that this person, organization or engine has contributed.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ActorProfile";
        final String               end2AttributeName            = "contributors";
        final String               end2AttributeDescription     = "Profiles of people, organizations or engines that defined this element.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "roleType";
        final String attribute1Description     = "Type of contribution.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("CrowdSourcingRole", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0120 Collections defines how to group related Referenceables together
     */
    private void add0120Collections()
    {
        this.archiveBuilder.addEnumDef(getOrderByEnum());

        this.archiveBuilder.addEntityDef(getCollectionEntity());

        this.archiveBuilder.addRelationshipDef(getCollectionMembershipRelationship());
        this.archiveBuilder.addRelationshipDef(getActorCollectionRelationship());

        this.archiveBuilder.addClassificationDef(getFolderClassification());
        this.archiveBuilder.addClassificationDef(getSetClassification());
    }


    private EnumDef getOrderByEnum()
    {
        final String guid            = "1d412439-4272-4a7e-a940-1065f889fc56";
        final String name            = "OrderBy";
        final String description     = "Defines the sequencing for a collection.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Name";
        final String element1Description     = "Order by name property.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Owner";
        final String element2Description     = "Order by owner property.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "DateAdded";
        final String element3Description     = "Order by date added to the metadata collection.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "DateUpdated";
        final String element4Description     = "Order by date that the asset was updated.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "DateCreated";
        final String element5Description     = "Order by date that the asset was created.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Order by another property.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getCollectionEntity()
    {
        final String guid            = "347005ba-2b35-4670-b5a7-12c9ebed0cf7";
        final String name            = "Collection";
        final String description     = "A group of related items.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the collection.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the collection.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getCollectionMembershipRelationship()
    {
        final String guid            = "5cabb76a-e25b-4bb5-8b93-768bbac005af";
        final String name            = "CollectionMember";
        final String description     = "Identifies a member of a collection.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Collection";
        final String               end1AttributeName            = "foundInCollections";
        final String               end1AttributeDescription     = "Collections that link to this element.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Referenceable";
        final String               end2AttributeName            = "members";
        final String               end2AttributeDescription     = "Members of this collection.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getActorCollectionRelationship()
    {
        final String guid            = "73cf5658-6a73-4ebc-8f4d-44fdfac0b437";
        final String name            = "ActorCollection";
        final String description     = "Identifies that a collection belongs to an actor profile.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ActorProfile";
        final String               end1AttributeName            = "consumingActors";
        final String               end1AttributeDescription     = "Profile owners that have linked to this collection.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Collection";
        final String               end2AttributeName            = "actorCollections";
        final String               end2AttributeDescription     = "Collections identified as of interest to the profile owner.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "collectionUse";
        final String attribute1Description     = "Description of how the collection is used, or why it is useful.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getFolderClassification()
    {
        final String guid            = "3c0fa687-8a63-4c8e-8bda-ede9c78be6c7";
        final String name            = "Folder";
        final String description     = "Defines that a collection should be treated like a file folder.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Collection";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "orderBy";
        final String attribute1Description     = "Definition for how elements in the collection should be ordered.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "otherPropertyName";
        final String attribute2Description     = "Name of property to use for ordering.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("OrderBy", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getSetClassification()
    {
        final String guid            = "3947f08d-7412-4022-81fc-344a20dfbb26";
        final String name            = "Set";
        final String description     = "Defines that a collection is an unordered set of items.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Collection";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0130 Projects describes the structure of a project and related collections.
     */
    private void add0130Projects()
    {
        this.archiveBuilder.addEntityDef(getProjectEntity());

        this.archiveBuilder.addRelationshipDef(getProjectHierarchyRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectDependencyRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectTeamRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectResourcesRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectScopeRelationship());

        this.archiveBuilder.addClassificationDef(getTaskClassification());
        this.archiveBuilder.addClassificationDef(getCampaignClassification());
    }


    private EntityDef getProjectEntity()
    {
        final String guid            = "0799569f-0c16-4a1f-86d9-e2e89568f7fd";
        final String name            = "Project";
        final String description     = "An organized activity, typically to achieve a well defined goal.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the project.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the project.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "startDate";
        final String attribute3Description     = "Start date of the project.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "plannedEndDate";
        final String attribute4Description     = "Planned completion data for the project.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "status";
        final String attribute5Description     = "Short description on current status of the project.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getProjectHierarchyRelationship()
    {
        final String guid            = "8f1134f6-b9fe-4971-bc57-6e1b8b302b55";
        final String name            = "ProjectHierarchy";
        final String description     = "A nesting relationship between projects.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "managingProject";
        final String               end1AttributeDescription     = "Project that oversees this project.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Project";
        final String               end2AttributeName            = "managedProject";
        final String               end2AttributeDescription     = "Project that this project is responsible for managing.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getProjectDependencyRelationship()
    {
        final String guid            = "5b6a56f1-68e2-4e10-85f0-fda47a4263fd";
        final String name            = "ProjectDependency";
        final String description     = "A dependency relationship between projects.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "dependentProject";
        final String               end1AttributeDescription     = "Projects that are dependent on this project.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Project";
        final String               end2AttributeName            = "dependsOnProjects";
        final String               end2AttributeDescription     = "Projects that are delivering resources or outcomes needed by this project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "dependencySummary";
        final String attribute1Description     = "Reasons for the project dependency.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getProjectTeamRelationship()
    {
        final String guid            = "746875af-2e41-4d1f-864b-35265df1d5dc";
        final String name            = "ProjectTeam";
        final String description     = "The team assigned to a project.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "projectFocus";
        final String               end1AttributeDescription     = "Projects that a team is working on.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Team";
        final String               end2AttributeName            = "supportingTeams";
        final String               end2AttributeDescription     = "Teams supporting this project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "teamRole";
        final String attribute1Description     = "Description of the role of the team in the project.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getProjectResourcesRelationship()
    {
        final String guid            = "03d25e7b-1c5b-4352-a472-33aa0ddcad4d";
        final String name            = "ProjectResources";
        final String description     = "A resource allocated for use in a project.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "projectUse";
        final String               end1AttributeDescription     = "Collections related to this project.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Collection";
        final String               end2AttributeName            = "supportingResources";
        final String               end2AttributeDescription     = "Collections of resource supporting the project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "resourceUse";
        final String attribute1Description     = "How the resources are being used by the project.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getProjectScopeRelationship()
    {
        final String guid            = "bc63ac45-b4d0-4fba-b583-92859de77dd8";
        final String name            = "ProjectScope";
        final String description     = "The documentation, assets and definitions that are affected by the project.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "projectsImpactingAssets";
        final String               end1AttributeDescription     = "The projects that are making changes to these collections of assets or related items.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Collection";
        final String               end2AttributeName            = "projectScope";
        final String               end2AttributeDescription     = "The collections of assets or related items that are being changed by this project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "scopeDescription";
        final String attribute1Description     = "Description of how each collection is being changed by the project.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getTaskClassification()
    {
        final String guid            = "2312b668-3670-4845-a140-ef88d5a6db0c";
        final String name            = "Task";
        final String description     = "A self-contained, short activity, typically for one or two people.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Project";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getCampaignClassification()
    {
        final String guid            = "41437629-8609-49ef-8930-8c435c912572";
        final String name            = "Campaign";
        final String description     = "A long-term strategic initiative that is implemented through multiple related projects.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Collection";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0135 Meetings defines how to record meetings and todos.
     */
    private void add0135Meetings()
    {
        this.archiveBuilder.addEnumDef(getToDoStatusEnum());

        this.archiveBuilder.addEntityDef(getMeetingEntity());
        this.archiveBuilder.addEntityDef(getToDoEntity());

        this.archiveBuilder.addRelationshipDef(getMeetingOnReferenceableRelationship());
        this.archiveBuilder.addRelationshipDef(getToDoSourceRelationship());
        this.archiveBuilder.addRelationshipDef(getTodoOnReferenceableRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectMeetingRelationship());
    }


    private EnumDef getToDoStatusEnum()
    {
        final String guid            = "7197ea39-334d-403f-a70b-d40231092df7";
        final String name            = "ToDoStatus";
        final String description     = "Progress on completing an action (to do).";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Open";
        final String element1Description     = "No action has been taken.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "InProgress";
        final String element2Description     = "Work is underway to complete the action.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Waiting";
        final String element3Description     = "Work is blocked waiting for resource of another action to complete.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Complete";
        final String element4Description     = "The action has been completed successfully.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Abandoned";
        final String element99Description     = "Work has stopped on the action and will not recommence.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getMeetingEntity()
    {
        final String guid            = "6bf90c79-32f4-47ad-959c-8fff723fe744";
        final String name            = "Meeting";
        final String description     = "Two or more people come together to discuss a topic, agree and action or exchange information.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "title";
        final String attribute1Description     = "Title of the meeting.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "startTime";
        final String attribute2Description     = "Start time of the meeting.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "endTime";
        final String attribute3Description     = "End time of the meeting.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "objective";
        final String attribute4Description     = "Reason for the meeting and intended outcome.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "minutes";
        final String attribute5Description     = "Description of what happened at the meeting.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getToDoEntity()
    {
        final String guid            = "93dbc58d-c826-4bc2-b36f-195148d46f86";
        final String name            = "ToDo";
        final String description     = "An action assigned to an individual.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "owner";
        final String attribute1Description     = "Person, organization or engine responsible for completing the action.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the required action.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "creationTime";
        final String attribute3Description     = "When the requested action was identified.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "status";
        final String attribute4Description     = "How complete is the action?";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "completionTime";
        final String attribute5Description     = "When the requested action was completed.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("ToDoStatus", attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getToDoSourceRelationship()
    {
        final String guid            = "a0b7ba50-4c97-4b76-9a7d-c6a00e1be646";
        final String name            = "ToDoSource";
        final String description     = "The source of a to do, such as a meeting or a condition detected by an engine.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "actionSource";
        final String               end1AttributeDescription     = "Source of the to do request.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ToDo";
        final String               end2AttributeName            = "actions";
        final String               end2AttributeDescription     = "Requests to perform actions related to this element.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getMeetingOnReferenceableRelationship()
    {
        final String guid            = "a05f918e-e7e2-419d-8016-5b37406df63a";
        final String name            = "MeetingOnReferenceable";
        final String description     = "A meeting about a specific project, deliverable, situation or plan of action.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Meeting";
        final String               end1AttributeName            = "meetings";
        final String               end1AttributeDescription     = "Related meetings.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Referenceable";
        final String               end2AttributeName            = "relatedReferenceables";
        final String               end2AttributeDescription     = "Elements that a relevant to this meeting.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getTodoOnReferenceableRelationship()
    {
        final String guid            = "aca1277b-bf1c-42f5-9b3b-fbc2c9047325";
        final String name            = "ToDoOnReferenceable";
        final String description     = "An action to change or support a specific project, deliverable, situation or plan of action.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "relatedReferenceables";
        final String               end1AttributeDescription     = "Elements affected by a to do request.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ToDo";
        final String               end2AttributeName            = "todos";
        final String               end2AttributeDescription     = "Potentially impacting requests for change.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getProjectMeetingRelationship()
    {
        final String guid            = "af2b5fab-8f83-4a2b-b749-1e6219f61f79";
        final String name            = "ProjectMeeting";
        final String description     = "A meeting organized as part of a specific project.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "relatedProjects";
        final String               end1AttributeDescription     = "Projects that are related to this meeting.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Meeting";
        final String               end2AttributeName            = "meetings";
        final String               end2AttributeDescription     = "Meetings related to this project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0140 Communities describe communities of people who have similar interests.
     */
    private void add0140Communities()
    {
        this.archiveBuilder.addEnumDef(getCommunityMembershipTypeEnum());

        this.archiveBuilder.addEntityDef(getCommunityEntity());

        this.archiveBuilder.addRelationshipDef(getCommunityMembershipRelationship());
        this.archiveBuilder.addRelationshipDef(getCommunityResourcesRelationship());
    }


    private EnumDef getCommunityMembershipTypeEnum()
    {
        final String guid            = "b0ef45bf-d12b-4b6f-add6-59c14648d750";
        final String name            = "CommunityMembershipType";
        final String description     = "Type of membership to a community.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Member";
        final String element1Description     = "Participant in the community.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Administrator";
        final String element2Description     = "Administrator of the community.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Leader";
        final String element3Description     = "Leader of the community.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Observer";
        final String element4Description     = "Observer of the community.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another role in the community.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getCommunityEntity()
    {
        final String guid            = "fbd42379-f6c3-4f08-b6f7-378565cda993";
        final String name            = "Community";
        final String description     = "A group of people with a common interest or skill.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the community.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the community.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "mission";
        final String attribute3Description     = "Purpose of the community.";
        final String attribute3DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getCommunityMembershipRelationship()
    {
        final String guid            = "7c7da1a3-01b3-473e-972e-606eff0cb112";
        final String name            = "CommunityMembership";
        final String description     = "Associates an actor profile with a community.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Community";
        final String               end1AttributeName            = "communities";
        final String               end1AttributeDescription     = "Communities that the owner of this profile is a member of.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ActorProfile";
        final String               end2AttributeName            = "members";
        final String               end2AttributeDescription     = "Members of the community.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "relationshipType";
        final String attribute1Description     = "Type of membership to the community.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("CommunityMembershipType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getCommunityResourcesRelationship()
    {
        final String guid            = "484d4fb9-4927-4926-8e6d-03e6c9885254";
        final String name            = "CommunityResources";
        final String description     = "Resources created or used by a community.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Community";
        final String               end1AttributeName            = "communityUses";
        final String               end1AttributeDescription     = "The communities that use this collection.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Collection";
        final String               end2AttributeName            = "supportingResources";
        final String               end2AttributeDescription     = "The collections of resources created or used by this community.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0150 Feedback provides all of the collaborative feedback and attachments that can be made by the user
     * community of the Assets.
     */
    private void add0150Feedback()
    {
        this.archiveBuilder.addEnumDef(getStarRatingEnum());
        this.archiveBuilder.addEnumDef(getCommentTypeEnum());

        this.archiveBuilder.addEntityDef(getRatingEntity());
        this.archiveBuilder.addEntityDef(getCommentEntity());
        this.archiveBuilder.addEntityDef(getLikeEntity());
        this.archiveBuilder.addEntityDef(getInformalTagEntity());
        this.archiveBuilder.addEntityDef(getPrivateTagEntity());

        this.archiveBuilder.addRelationshipDef(getAttachedRatingRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedCommentRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedLikeRelationship());
        this.archiveBuilder.addRelationshipDef(getAcceptedAnswerRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedTagRelationship());
    }


    private EnumDef getStarRatingEnum()
    {
        final String guid            = "77fea3ef-6ec1-4223-8408-38567e9d3c93";
        final String name            = "StarRating";
        final String description     = "Level of support or appreciation for an item.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "NotRecommended";
        final String element1Description     = "This content is not recommended.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "OneStar";
        final String element2Description     = "One star rating.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "TwoStar";
        final String element3Description     = "Two star rating.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "ThreeStar";
        final String element4Description     = "Three star rating.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "FourStar";
        final String element5Description     = "Four star rating.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "FiveStar";
        final String element6Description     = "Five star rating.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getCommentTypeEnum()
    {
        final String guid            = "06d5032e-192a-4f77-ade1-a4b97926e867";
        final String name            = "CommentType";
        final String description     = "Descriptor for a comment that indicated its intent.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "GeneralComment";
        final String element1Description     = "General comment.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Question";
        final String element2Description     = "A question.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Answer";
        final String element3Description     = "An answer to a previously asked question.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Suggestion";
        final String element4Description     = "A suggestion for improvement.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 3;
        final String element5Value           = "Experience";
        final String element5Description     = "An account of an experience.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getRatingEntity()
    {
        final String guid            = "7299d721-d17f-4562-8286-bcd451814478";
        final String name            = "Rating";
        final String description     = "Quantitative feedback related to an item.";
        final String descriptionGUID = null;

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                null,
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute2Name            = "stars";
        final String attribute2Description     = "Rating level provided.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "review";
        final String attribute3Description     = "Additional comments associated with the rating.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("StarRating", attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getCommentEntity()
    {
        final String guid            = "1a226073-9c84-40e4-a422-fbddb9b84278";
        final String name            = "Comment";
        final String description     = "Descriptive feedback or discussion related to an item.";
        final String descriptionGUID = null;

        final String superType = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superType),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "comment";
        final String attribute1Description     = "Feedback comments or additional information.";
        final String attribute1DescriptionGUID = null;
        final String attribute3Name            = "commentType";
        final String attribute3Description     = "classification for the comment.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("CommentType", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getLikeEntity()
    {
        final String guid            = "deaa5ca0-47a0-483d-b943-d91c76744e01";
        final String name            = "Like";
        final String description     = "Boolean type of rating expressing a favorable impression.";
        final String descriptionGUID = null;

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 null,
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getInformalTagEntity()
    {
        final String guid            = "ba846a7b-2955-40bf-952b-2793ceca090a";
        final String name            = "InformalTag";
        final String description     = "An descriptive tag for an item.";
        final String descriptionGUID = null;

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                null,
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute2Name            = "tagName";
        final String attribute2Description     = "Descriptive name of the tag.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "tagDescription";
        final String attribute3Description     = "More detail on the meaning of the tag.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getPrivateTagEntity()
    {
        final String guid            = "9b3f5443-2475-4522-bfda-8f1f17e9a6c3";
        final String name            = "PrivateTag";
        final String description     = "A descriptive tag for an item that is visible only to the creator.";
        final String descriptionGUID = null;

        final String superTypeName = "InformalTag";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getAttachedRatingRelationship()
    {
        final String guid            = "0aaad9e9-9cc5-4ad8-bc2e-c1099bab6344";
        final String name            = "AttachedRating";
        final String description     = "Links a rating to an item.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "ratedElement";
        final String               end1AttributeDescription     = "Element that is rated.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Rating";
        final String               end2AttributeName            = "starRatings";
        final String               end2AttributeDescription     = "Accumulated ratings.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAttachedCommentRelationship()
    {
        final String guid            = "0d90501b-bf29-4621-a207-0c8c953bdac9";
        final String name            = "AttachedComment";
        final String description     = "Links a comment to an item, or another comment.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "commentOnElement";
        final String               end1AttributeDescription     = "Element that this comment relates.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Comment";
        final String               end2AttributeName            = "comments";
        final String               end2AttributeDescription     = "Accumulated comments.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAttachedLikeRelationship()
    {
        final String guid            = "e2509715-a606-415d-a995-61d00503dad4";
        final String name            = "AttachedLike";
        final String description     = "Links a like to an item.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "likedElement";
        final String               end1AttributeDescription     = "Element that is liked.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Like";
        final String               end2AttributeName            = "likes";
        final String               end2AttributeDescription     = "Accumulated likes.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAcceptedAnswerRelationship()
    {
        final String guid            = "ecf1a3ca-adc5-4747-82cf-10ec590c5c69";
        final String name            = "AcceptedAnswer";
        final String description     = "Identifies a comment as answering a question asked in another comment.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Comment";
        final String               end1AttributeName            = "answeredQuestions";
        final String               end1AttributeDescription     = "Questions that now has an accepted answer.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_LEAST_ONE_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Comment";
        final String               end2AttributeName            = "acceptedAnswers";
        final String               end2AttributeDescription     = "Accumulated answers.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAttachedTagRelationship()
    {
        final String guid            = "4b1641c4-3d1a-4213-86b2-d6968b6c65ab";
        final String name            = "AttachedTag";
        final String description     = "Links an informal tag to an item.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "taggedElement";
        final String               end1AttributeDescription     = "Element that is tagged.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "InformalTag";
        final String               end2AttributeName            = "tags";
        final String               end2AttributeDescription     = "Accumulated tags.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0160 Notes describes notelogs and notes within them.  Notes are kept but the owners/stewards working on the
     * Assets.
     */
    private void add0160Notes()
    {
        this.archiveBuilder.addEntityDef(getNoteEntryEntity());
        this.archiveBuilder.addEntityDef(getNoteLogEntity());

        this.archiveBuilder.addRelationshipDef(getAttachedNoteLogRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedNoteLogEntryRelationship());
    }


    private EntityDef getNoteEntryEntity()
    {
        final String guid            = "2a84d94c-ac6f-4be1-a72a-07dcec7b1fe3";
        final String name            = "NoteEntry";
        final String description     = "An entry in a note log.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "text";
        final String attribute1Description     = "Text of the note entry.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getNoteLogEntity()
    {
        final String guid            = "646727c7-9ad4-46fa-b660-265489ad96c6";
        final String name            = "NoteLog";
        final String description     = "An ordered list of related notes.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the note log.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the note log.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getAttachedNoteLogRelationship()
    {
        final String guid            = "4f798c0c-6769-4a2d-b489-d2714d89e0a4";
        final String name            = "AttachedNoteLog";
        final String description     = "Links a note log to an item.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "describes";
        final String               end1AttributeDescription     = "Subject of the note log.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "NoteLog";
        final String               end2AttributeName            = "noteLogs";
        final String               end2AttributeDescription     = "Log of related notes.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAttachedNoteLogEntryRelationship()
    {
        final String guid            = "38edecc6-f385-4574-8144-524a44e3e712";
        final String name            = "AttachedNoteLogEntry";
        final String description     = "Link between a note log and one of its note log entries.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "NoteLog";
        final String               end1AttributeName            = "logs";
        final String               end1AttributeDescription     = "Logs that this entry relates.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_LEAST_ONE_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "NoteEntry";
        final String               end2AttributeName            = "entries";
        final String               end2AttributeDescription     = "Accumulated notes.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * ========================================
     * AREA 2 - connectors and assets
     */

    /**
     * Area 2 covers the different types of Assets and the Connection information used by the Open Connector Framework
     * (OCF).
     */
    private void addArea2Types()
    {
        this.add0201ConnectorsAndConnections();
        this.add0205ConnectionLinkage();
        this.add0210DataStores();
        this.add0212DeployedAPIs();
        this.add0215SoftwareComponents();
        this.add0217AutomatedProcesses();
        this.add0220FilesAndFolders();
        this.add0221DocumentStores();
        this.add0222GraphStores();
        this.add0223EventsAndLogs();
        this.add0224Databases();
        this.add0225MetadataRepositories();
        this.add0227Keystores();
        this.add0230CodeTables();
        this.add0235InformationView();
        this.add0239Reports();
        this.add0240ApplicationsAndProcesses();
        this.add0250DataProcessingEngines();
        this.add0260Transformations();
        this.add0265AnalyticsAssets();
        this.add0270IoTAssets();
        this.add0280ModelAssets();
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0201 Connectors and Connections defines the details of the Connection that describes the connector type
     * and endpoint for a specific connector instance.
     */
    private void add0201ConnectorsAndConnections()
    {
        this.archiveBuilder.addEntityDef(getConnectionEntity());
        this.archiveBuilder.addEntityDef(getConnectorTypeEntity());

        this.archiveBuilder.addRelationshipDef(getConnectionEndpointRelationship());
        this.archiveBuilder.addRelationshipDef(getConnectionConnectorTypeRelationship());
    }


    private EntityDef getConnectionEntity()
    {
        final String guid            = "114e9f8f-5ff3-4c32-bd37-a7eb42712253";
        final String name            = "Connection";
        final String description     = "A set of properties to identify and configure a connector instance.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for the connection, suitable for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the connection.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "securedProperties";
        final String attribute3Description     = "Private properties accessible only to the connector.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getConnectorTypeEntity()
    {
        final String guid            = "954421eb-33a6-462d-a8ca-b5709a1bd0d4";
        final String name            = "ConnectorType";
        final String description     = "A set of properties describing a type of connector.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for the connector type, suitable for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the connector type.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "connectorProviderClassName";
        final String attribute3Description     = "Name of the Java class that implements this connector type's open connector framework (OCF) connector provider.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getConnectionEndpointRelationship()
    {
        final String guid            = "887a7132-d6bc-4b92-a483-e80b60c86fb2";
        final String name            = "ConnectionEndpoint";
        final String description     = "A link between a connection and the endpoint that the connector should use.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Endpoint";
        final String               end1AttributeName            = "connectionEndpoint";
        final String               end1AttributeDescription     = "Server endpoint that provides access to the asset.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Connection";
        final String               end2AttributeName            = "connections";
        final String               end2AttributeDescription     = "Connections to this endpoint.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getConnectionConnectorTypeRelationship()
    {
        final String guid            = "e542cfc1-0b4b-42b9-9921-f0a5a88aaf96";
        final String name            = "ConnectionConnectorType";
        final String description     = "A link between a connection and the connector type that should be used.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Connection";
        final String               end1AttributeName            = "connections";
        final String               end1AttributeDescription     = "Connections using this connector type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ConnectorType";
        final String               end2AttributeName            = "connectorType";
        final String               end2AttributeDescription     = "Type of connector to use for the asset.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0205 Connection Links defines the relationship between the connection and an Asset, plus the nesting
     * of connections for information virtualization support.
     */
    private void add0205ConnectionLinkage()
    {
        this.archiveBuilder.addEntityDef(getVirtualConnectionEntity());

        this.archiveBuilder.addRelationshipDef(getEmbeddedConnectionRelationship());
        this.archiveBuilder.addRelationshipDef(getConnectionToAssetRelationship());
    }


    private EntityDef getVirtualConnectionEntity()
    {
        final String guid            = "82f9c664-e59d-484c-a8f3-17088c23a2f3";
        final String name            = "VirtualConnection";
        final String description     = "A connector for a virtual resource that needs to retrieve data from multiple places.";
        final String descriptionGUID = null;

        final String superTypeName = "Connection";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getEmbeddedConnectionRelationship()
    {
        final String guid            = "eb6dfdd2-8c6f-4f0d-a17d-f6ce4799f64f";
        final String name            = "EmbeddedConnection";
        final String description     = "A link between a virtual connection and one of the connections it depends on.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "VirtualConnection";
        final String               end1AttributeName            = "supportingVirtualConnections";
        final String               end1AttributeDescription     = "Virtual connections using this connection.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Connection";
        final String               end2AttributeName            = "embeddedConnections";
        final String               end2AttributeDescription     = "Connections embedded in this virtual connection.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "arguments";
        final String attribute1Description     = "Additional arguments needed by the virtual connector when using each connection.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getConnectionToAssetRelationship()
    {
        final String guid            = "e777d660-8dbe-453e-8b83-903771f054c0";
        final String name            = "ConnectionToAsset";
        final String description     = "Link between a connection and the description of the asset it can be used to access.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Connection";
        final String               end1AttributeName            = "connections";
        final String               end1AttributeDescription     = "Connections to this asset.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Asset";
        final String               end2AttributeName            = "asset";
        final String               end2AttributeDescription     = "Asset that can be accessed with this connection.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "assetSummary";
        final String attribute1Description     = "Description of the asset that is retrieved through this connection.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0210 DataStores describe physical data store assets.
     */
    private void add0210DataStores()
    {
        this.archiveBuilder.addEntityDef(getDataStoreEntity());

        this.archiveBuilder.addRelationshipDef(getDataContentForDataSetRelationship());

        this.archiveBuilder.addClassificationDef(getDataStoreEncodingClassification());
    }


    private EntityDef getDataStoreEntity()
    {
        final String guid            = "30756d0b-362b-4bfa-a0de-fce6a8f47b47";
        final String name            = "DataStore";
        final String description     = "A physical store of data.";
        final String descriptionGUID = null;

        final String superTypeName = "Asset";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "createTime";
        final String attribute1Description     = "Creation time of the data store.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "modifiedTime";
        final String attribute2Description     = "Last known modification time.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getDataContentForDataSetRelationship()
    {
        final String guid            = "b827683c-2924-4df3-a92d-7be1888e23c0";
        final String name            = "DataContentForDataSet";
        final String description     = "The assets that provides data for a data set.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.BOTH;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Asset";
        final String               end1AttributeName            = "dataContent";
        final String               end1AttributeDescription     = "Assets supporting a data set.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataSet";
        final String               end2AttributeName            = "supportedDataSets";
        final String               end2AttributeDescription     = "Data sets that use this asset.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getDataStoreEncodingClassification()
    {
        final String guid            = "f08e48b5-6b66-40f5-8ff6-c2bfe527330b";
        final String name            = "DataStoreEncoding";
        final String description     = "Description for how data is organized and represented in a data store.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "encoding";
        final String attribute1Description     = "Encoding type.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "language";
        final String attribute2Description     = "Language used in the encoding.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "description";
        final String attribute3Description     = "Description the encoding.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person, organization or engine that defined the encoding.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Source of the encoding.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "properties";
        final String attribute6Description     = "Additional properties for the encoding.";
        final String attribute6DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0212 Deployed APIs defines an API that has been deployed to IT Infrastructure
     */
    private void add0212DeployedAPIs()
    {
        this.archiveBuilder.addEntityDef(getDeployedAPIEntity());

        this.archiveBuilder.addRelationshipDef(getAPIEndpointRelationship());

        this.archiveBuilder.addClassificationDef(getRequestResponseInterfaceClassification());
        this.archiveBuilder.addClassificationDef(getListenerInterfaceClassification());
        this.archiveBuilder.addClassificationDef(getPublisherInterfaceClassification());
    }


    private EntityDef getDeployedAPIEntity()
    {
        final String guid            = "7dbb3e63-138f-49f1-97b4-66313871fc14";
        final String name            = "DeployedAPI";
        final String description     = "A callable interface running at an endpoint.";
        final String descriptionGUID = null;

        final String superTypeName = "Asset";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getAPIEndpointRelationship()
    {
        final String guid            = "de5b9501-3ad4-4803-a8b2-e311c72a4336";
        final String name            = "APIEndpoint";
        final String description     = "The endpoint for a deployed API.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "DeployedAPI";
        final String               end1AttributeName            = "supportedAPIs";
        final String               end1AttributeDescription     = "APIs that can be called from this endpoint.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Endpoint";
        final String               end2AttributeName            = "accessEndpoints";
        final String               end2AttributeDescription     = "Endpoints used to call this API.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getRequestResponseInterfaceClassification()
    {
        final String guid            = "14a29330-e830-4343-a41e-d57e2cec82f8";
        final String name            = "RequestResponseInterface";
        final String description     = "Identifies an API that supports a request response interaction style.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DeployedAPI";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getListenerInterfaceClassification()
    {
        final String guid            = "4099d2ed-2a5e-4c44-8443-9de4e378a4ba";
        final String name            = "ListenerInterface";
        final String description     = "Identifies an API that listens for incoming events and processes them.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DeployedAPI";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getPublisherInterfaceClassification()
    {
        final String guid            = "4fdedcd5-b186-4bee-887a-02fa29a10750";
        final String name            = "PublisherInterface";
        final String description     = "Identifies an API that sends out events to other listening components.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DeployedAPI";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0215 Software Components defines a generic Asset for a software component.
     */
    private void add0215SoftwareComponents()
    {
        this.archiveBuilder.addEntityDef(getSoftwareComponentsEntity());

        this.archiveBuilder.addRelationshipDef(getSoftwareComponentDeploymentRelationship());
    }


    private EntityDef getSoftwareComponentsEntity()
    {
        final String guid            = "486af62c-dcfd-4859-ab24-eab2e380ecfd";
        final String name            = "SoftwareComponent";
        final String description     = "A packaged software component supporting a well-defined function.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getSoftwareComponentDeploymentRelationship()
    {
        final String guid            = "de2d7f2e-1759-44e3-b8a6-8af53e8fb0ee";
        final String name            = "SoftwareComponentDeployment";
        final String description     = "Identifies where a software component is deployed.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SoftwareServerCapability";
        final String               end1AttributeName            = "deploymentLocations";
        final String               end1AttributeDescription     = "Places where this software component is deployed.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SoftwareComponent";
        final String               end2AttributeName            = "deployedComponents";
        final String               end2AttributeDescription     = "Software components that implement this software server capability.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "deploymentDate";
        final String attribute1Description     = "Date that the component was last deployed.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "deploymentProcess";
        final String attribute2Description     = "Process used to deploy the component.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0217 Automated Processes defines a Process is automated (as opposed to manual).
     */
    private void add0217AutomatedProcesses()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0220 Files and Folders provides the definitions for describing filesystems and their content
     */
    private void add0220FilesAndFolders()
    {
        this.archiveBuilder.addEntityDef(getFileFolderEntity());
        this.archiveBuilder.addEntityDef(getDataFileEntity());

        this.archiveBuilder.addRelationshipDef(getFolderHierarchyRelationship());
        this.archiveBuilder.addRelationshipDef(getNestedFileRelationship());
        this.archiveBuilder.addRelationshipDef(getLinkedFileRelationship());

        this.archiveBuilder.addClassificationDef(getFileSystemClassification());
    }


    private EntityDef getFileFolderEntity()
    {
        final String guid            = "229ed5cc-de31-45fc-beb4-9919fd247398";
        final String name            = "FileFolder";
        final String description     = "A description of a folder (directory) in a file system.";
        final String descriptionGUID = null;

        final String superTypeName = "DataStore";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getDataFileEntity()
    {
        final String guid            = "10752b4a-4b5d-4519-9eae-fdd6d162122f";
        final String name            = "DataFile";
        final String description     = "A description of a file containing data stored in a file system.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getFolderHierarchyRelationship()
    {
        final String guid            = "48ac9028-45dd-495d-b3e1-622685b54a01";
        final String name            = "FolderHierarchy";
        final String description     = "A nested relationship between two file folders.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "FileFolder";
        final String               end1AttributeName            = "parentFolder";
        final String               end1AttributeDescription     = "Parent folder.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "FileFolder";
        final String               end2AttributeName            = "nestedFolder";
        final String               end2AttributeDescription     = "Folders embedded in this folder.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getNestedFileRelationship()
    {
        final String guid            = "4cb88900-1446-4eb6-acea-29cd9da45e63";
        final String name            = "NestedFile";
        final String description     = "The location of a data file within a file folder in.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "FileFolder";
        final String               end1AttributeName            = "homeFolder";
        final String               end1AttributeDescription     = "Identifies the location of this datafile.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataFile";
        final String               end2AttributeName            = "nestedFiles";
        final String               end2AttributeDescription     = "Files stored in this folder.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getLinkedFileRelationship()
    {
        final String guid            = "970a3405-fde1-4039-8249-9aa5f56d5151";
        final String name            = "LinkedFile";
        final String description     = "A data file that is linked to a file folder (rather than stored in it).";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "FileFolder";
        final String               end1AttributeName            = "linkedFolders";
        final String               end1AttributeDescription     = "Folders that this file is linked to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataFile";
        final String               end2AttributeName            = "linkedFiles";
        final String               end2AttributeDescription     = "Files linked to the folder.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getFileSystemClassification()
    {
        final String guid            = "cab5ba1d-cfd3-4fca-857d-c07711fc4157";
        final String name            = "FileSystem";
        final String description     = "A server that support a file system containing a hierarchy of file folders and data files.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "format";
        final String attribute1Description     = "Format of the file system.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "encryption";
        final String attribute2Description     = "Level of encryption used on the filesystem (if any).";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0221 Document Stores define both simple document stores and content management systems
     */
    private void add0221DocumentStores()
    {
        this.archiveBuilder.addEntityDef(getMediaFileEntity());
        this.archiveBuilder.addEntityDef(getMediaCollectionEntity());
        this.archiveBuilder.addEntityDef(getDocumentEntity());

        this.archiveBuilder.addRelationshipDef(getGroupedMediaRelationship());
        this.archiveBuilder.addRelationshipDef(getLinkedMediaRelationship());

        this.archiveBuilder.addClassificationDef(getContentManagerClassification());
        this.archiveBuilder.addClassificationDef(getDocumentStoreClassification());
    }


    private EntityDef getMediaFileEntity()
    {
        final String guid            = "c5ce5499-9582-42ea-936c-9771fbd475f8";
        final String name            = "MediaFile";
        final String description     = "A data file containing unstructured data.";
        final String descriptionGUID = null;

        final String superTypeName = "DataFile";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "embeddedMetadata";
        final String attribute1Description     = "Metadata properties embedded in the media file.";
        final String attribute1DescriptionGUID = null;


        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);


        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getMediaCollectionEntity()
    {
        final String guid            = "0075d603-1627-41c5-8cae-f5458d1247fe";
        final String name            = "MediaCollection";
        final String description     = "A group of related media files.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getDocumentEntity()
    {
        final String guid            = "b463827c-c0a0-4cfb-a2b2-ddc63746ded4";
        final String name            = "Document";
        final String description     = "A data file containing unstructured text.";
        final String descriptionGUID = null;

        final String superTypeName = "MediaFile";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getGroupedMediaRelationship()
    {
        final String guid            = "7d881574-461d-475c-ab44-077451528cb8";
        final String name            = "GroupedMedia";
        final String description     = "Links a media file into a data set.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "MediaCollection";
        final String               end1AttributeName            = "dataSetMembership";
        final String               end1AttributeDescription     = "Identifies the data sets this media file belongs to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "MediaFile";
        final String               end2AttributeName            = "dataSetMembers";
        final String               end2AttributeDescription     = "Media files that make up this media collection.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getLinkedMediaRelationship()
    {
        final String guid            = "cee3a190-fc8d-4e53-908a-f1b9689581e0";
        final String name            = "LinkedMedia";
        final String description     = "Links a media file to another media file and describes relationship.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "MediaFile";
        final String               end1AttributeName            = "linkedMediaFiles";
        final String               end1AttributeDescription     = "Link to related media files.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "MediaFile";
        final String               end2AttributeName            = "linkedMediaFiles";
        final String               end2AttributeDescription     = "Link to related media files.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getContentManagerClassification()
    {
        final String guid            = "fa4df7b5-cb6d-475c-889e-8f3b7ca564d3";
        final String name            = "ContentManager";
        final String description     = "Identifies a server as a manager of controlled documents and related media.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getDocumentStoreClassification()
    {
        final String guid            = "37156790-feac-4e1a-a42e-88858ae6f8e1";
        final String name            = "DocumentStore";
        final String description     = "Identifies a data store as one that contains documents.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0222GraphStores()
    {
        this.archiveBuilder.addClassificationDef(getGraphStoreClassification());
    }


    private ClassificationDef getGraphStoreClassification()
    {
        final String guid            = "86de3633-eec8-4bf9-aad1-e92df1ca2024";
        final String name            = "GraphStore";
        final String description     = "Identifies a data store as one that contains one or more graphs.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of graph store.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0223 Events and Logs describes events, log files and event management.
     */
    private void add0223EventsAndLogs()
    {
        this.archiveBuilder.addEntityDef(getSubscriberListEntity());
        this.archiveBuilder.addEntityDef(getTopicEntity());

        this.archiveBuilder.addRelationshipDef(getTopicSubscribersRelationship());

        this.archiveBuilder.addClassificationDef(getLogFileClassification());
        this.archiveBuilder.addClassificationDef(getNotificationManagerClassification());
    }


    private EntityDef getSubscriberListEntity()
    {
        final String guid            = "69751093-35f9-42b1-944b-ba6251ff513d";
        final String name            = "SubscriberList";
        final String description     = "A data set containing a list of endpoints registered to receive events from a topic.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getTopicEntity()
    {
        final String guid            = "29100f49-338e-4361-b05d-7e4e8e818325";
        final String name            = "Topic";
        final String description     = "A location for storing and distributing related events.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "topicType";
        final String attribute1Description     = "Type of topic.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getTopicSubscribersRelationship()
    {
        final String guid            = "bc91a28c-afb9-41a7-8eb2-fc8b5271fe9e";
        final String name            = "TopicSubscribers";
        final String description     = "Links the list of subscribers to a topic.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SubscriberList";
        final String               end1AttributeName            = "subscribers";
        final String               end1AttributeDescription     = "The endpoints subscribed to this topic.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Topic";
        final String               end2AttributeName            = "topics";
        final String               end2AttributeDescription     = "The topics used by this subscriber list.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getLogFileClassification()
    {
        final String guid            = "ff4c8484-9127-464a-97fc-99579d5bc429";
        final String name            = "LogFile";
        final String description     = "Identifies a data file as one containing log records.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataFile";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of log file.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getNotificationManagerClassification()
    {
        final String guid            = "3e7502a7-396a-4737-a106-378c9c94c105";
        final String name            = "NotificationManager";
        final String description     = "Identifies a server capability that is distributing events from a topic to its subscriber list.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServerCapability";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0224 Databases describe database servers
     */
    private void add0224Databases()
    {
        this.archiveBuilder.addEntityDef(getDeployedDatabaseSchemaEntity());

        this.archiveBuilder.addClassificationDef(getDatabaseClassification());
        this.archiveBuilder.addClassificationDef(getDatabaseServerClassification());
    }


    private EntityDef getDeployedDatabaseSchemaEntity()
    {
        final String guid            = "eab811ec-556a-45f1-9091-bc7ac8face0f";
        final String name            = "DeployedDatabaseSchema";
        final String description     = "A collection of database tables and views running in a database server.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private ClassificationDef getDatabaseClassification()
    {
        final String guid            = "0921c83f-b2db-4086-a52c-0d10e52ca078";
        final String name            = "Database";
        final String description     = "A data store containing relational data.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of database.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getDatabaseServerClassification()
    {
        final String guid            = "6bb58cc9-ed9e-4f75-b2f2-6d308554eb52";
        final String name            = "DatabaseServer";
        final String description     = "Identifies a server as one that manages one or more databases.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of database server.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "version";
        final String attribute2Description     = "Version of the database server software.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "source";
        final String attribute3Description     = "Source of the database software.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0225MetadataRepositories()
    {
        this.archiveBuilder.addEntityDef(getEnterpriseAccessLayerEntity());
        this.archiveBuilder.addEntityDef(getCohortMemberEntity());
        this.archiveBuilder.addEntityDef(getMetadataRepositoryCohortEntity());
        this.archiveBuilder.addEntityDef(getMetadataCollectionEntity());

        this.archiveBuilder.addRelationshipDef(getMetadataCohortPeerRelationship());
        this.archiveBuilder.addRelationshipDef(getCohortMemberMetadataCollectionRelationship());

        this.archiveBuilder.addClassificationDef(getMetadataServerClassification());
        this.archiveBuilder.addClassificationDef(getRepositoryProxyClassification());
        this.archiveBuilder.addClassificationDef(getMetadataRepositoryClassification());
        this.archiveBuilder.addClassificationDef(getCohortRegistryStoreClassification());
    }


    private EntityDef getEnterpriseAccessLayerEntity()
    {
        final String guid            = "39444bf9-638e-4124-a5f9-1b8f3e1b008b";
        final String name            = "EnterpriseAccessLayer";
        final String description     = "Repository services for the open metadata access services (OMAS).";
        final String descriptionGUID = null;

        final String superTypeName = "SoftwareServerCapability";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "topicRoot";
        final String attribute1Description     = "Root of topic names used by the Open Metadata access Services (OMASs).";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "metadataCollectionId";
        final String attribute2Description     = "Unique identifier for the metadata collection accessed through this enterprise access layer.";
        final String attribute2DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getCohortMemberEntity()
    {
        final String guid            = "42063797-a78a-4720-9353-52026c75f667";
        final String name            = "CohortMember";
        final String description     = "A data set containing details of the members of an open metadata cohort.";
        final String descriptionGUID = null;

        final String superTypeName = "SoftwareServerCapability";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "version";
        final String attribute1Description     = "Version number of the protocol supported by the cohort registry.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getMetadataRepositoryCohortEntity()
    {
        final String guid            = "43e7dca2-c7b4-4cdf-a1ea-c9d4f7093893";
        final String name            = "MetadataRepositoryCohort";
        final String description     = "A group of collaborating open metadata repositories.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the scope of the open metadata repository cohort.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "topic";
        final String attribute2Description     = "Name of the topic used to exchange registration, type definitions and metadata instances between the members of the open metadata repository cohort.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getMetadataCollectionEntity()
    {
        final String guid            = "ea3b15af-ed0e-44f7-91e4-bdb299dd4976";
        final String name            = "MetadataCollection";
        final String description     = "A data set containing metadata.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "metadataCollectionId";
        final String attribute1Description     = "Unique identifier for the metadata collection managed in the local repository.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getMetadataCohortPeerRelationship()
    {
        final String guid            = "954cdba1-3d69-4db1-bf0e-d59fd2c25a27";
        final String name            = "MetadataCohortPeer";
        final String description     = "A metadata repository's registration with an open metadata cohort.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "MetadataRepositoryCohort";
        final String               end1AttributeName            = "registeredWithCohorts";
        final String               end1AttributeDescription     = "Identifies which cohorts this cohort member is registered with.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "CohortMember";
        final String               end2AttributeName            = "cohortMembership";
        final String               end2AttributeDescription     = "Members of this cohort.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "registrationDate";
        final String attribute1Description     = "Date first registered with the cohort.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getDateTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getCohortMemberMetadataCollectionRelationship()
    {
        final String guid            = "8b9dd3ea-057b-4709-9b42-f16098523907";
        final String name            = "CohortMemberMetadataCollection";
        final String description     = "The local metadata collection associated with a cohort peer.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "CohortMember";
        final String               end1AttributeName            = "cohortMember";
        final String               end1AttributeDescription     = "Cohort registry representing this metadata collection on the metadata highway.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "MetadataCollection";
        final String               end2AttributeName            = "localMetadataCollection";
        final String               end2AttributeDescription     = "Metadata to exchange with the cohorts.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getMetadataServerClassification()
    {
        final String guid            = "74a256ad-4022-4518-a446-c65fe082d4d3";
        final String name            = "MetadataServer";
        final String description     = "A server hosting a metadata collection.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "format";
        final String attribute1Description     = "format of supported metadata.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "type";
        final String attribute2Description     = "Type of metadata server.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getRepositoryProxyClassification()
    {
        final String guid            = "ae81c35e-7078-46f0-9b2c-afc99accf3ec";
        final String name            = "RepositoryProxy";
        final String description     = "A server acting as an open metadata adapter for a metadata repository.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of repository proxy.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getMetadataRepositoryClassification()
    {
        final String guid            = "c40397bd-eab0-4b2e-bffb-e7fa0f93a5a9";
        final String name            = "MetadataRepository";
        final String description     = "A data store containing metadata.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of metadata repository.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCohortRegistryStoreClassification()
    {
        final String guid            = "2bfdcd0d-68bb-42c3-ae75-e9fb6c3dff70";
        final String name            = "CohortRegistryStore";
        final String description     = "A data store containing cohort membership registration details.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataStore";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0227Keystores()
    {
        this.archiveBuilder.addEntityDef(getKeystoreFileEntity());
        this.archiveBuilder.addEntityDef(getKeystoreCollectionEntity());
    }


    private EntityDef getKeystoreFileEntity()
    {
        final String guid            = "17bee904-5b35-4c81-ac63-871c615424a2";
        final String name            = "KeystoreFile";
        final String description     = "An encrypted data store containing authentication and related security information.";
        final String descriptionGUID = null;

        final String superTypeName = "DataFile";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getKeystoreCollectionEntity()
    {
        final String guid            = "979d97dd-6782-4648-8e2a-8982994533e6";
        final String name            = "KeyStoreCollection";
        final String description     = "A data set containing authentication and related security information.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0230CodeTables()
    {
        this.archiveBuilder.addEntityDef(getReferenceCodeTableEntity());
        this.archiveBuilder.addEntityDef(getReferenceCodeMappingTableEntity());

    }


    private EntityDef getReferenceCodeTableEntity()
    {
        final String guid            = "201f48c5-4e4b-41dc-9c5f-0bc9742190cf";
        final String name            = "ReferenceCodeTable";
        final String description     = "A data set containing code values and their translations.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getReferenceCodeMappingTableEntity()
    {
        final String guid            = "9c6ec0c6-0b26-4414-bffe-089144323213";
        final String name            = "ReferenceCodeMappingTable";
        final String description     = "A data set containing mappings between code values from different data sets.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0235InformationView()
    {
        this.archiveBuilder.addEntityDef(getInformationViewEntity());

    }


    private EntityDef getInformationViewEntity()
    {
        final String guid            = "68d7b905-6438-43be-88cf-5de027b4aaaf";
        final String name            = "InformationView";
        final String description     = "A data set containing selected data items from one or more data stores or data sets.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0239Reports()
    {
        this.archiveBuilder.addEntityDef(getFormEntity());
        this.archiveBuilder.addEntityDef(getReportEntity());
    }


    private EntityDef getFormEntity()
    {
        final String guid            = "8078e3d1-0c63-4ace-aafa-68498b39ccd6";
        final String name            = "Form";
        final String description     = "A collection of data items used to request activity.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getReportEntity()
    {
        final String guid            = "e9077f4f-955b-4d7b-b1f7-12ee769ee0c3";
        final String name            = "Report";
        final String description     = "A collection if data items that describe a situation.";
        final String descriptionGUID = null;

        final String superTypeName = "DataSet";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0240ApplicationsAndProcesses()
    {
        this.archiveBuilder.addEntityDef(getApplicationEntity());

        this.archiveBuilder.addRelationshipDef(getRuntimeForProcessRelationship());

        this.archiveBuilder.addClassificationDef(getApplicationServerClassification());
        this.archiveBuilder.addClassificationDef(getWebserverClassification());
    }


    private EntityDef getApplicationEntity()
    {
        final String guid            = "58280f3c-9d63-4eae-9509-3f223872fb25";
        final String name            = "Application";
        final String description     = "A server capability supporting a specific business function.";
        final String descriptionGUID = null;

        final String superTypeName = "SoftwareServerCapability";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getRuntimeForProcessRelationship()
    {
        final String guid            = "f6b5cf4f-7b88-47df-aeb0-d80d28ba1ec1";
        final String name            = "RuntimeForProcess";
        final String description     = "Identifies the deployed application that supports a specific automated process.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Application";
        final String               end1AttributeName            = "implementingApplication";
        final String               end1AttributeDescription     = "Application that contains the process implementation.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Process";
        final String               end2AttributeName            = "implementedProcesses";
        final String               end2AttributeDescription     = "Processes that are implemented by this application.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getApplicationServerClassification()
    {
        final String guid            = "19196efb-2706-47bf-8e51-e8ba5b36d033";
        final String name            = "ApplicationServer";
        final String description     = "A server that hosts applications.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getWebserverClassification()
    {
        final String guid            = "d13e1cc5-bb7e-41ec-8233-9647fbf92a19";
        final String name            = "Webserver";
        final String description     = "A server that supports HTTP-based application such as websites and REST services.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0250DataProcessingEngines()
    {
        this.archiveBuilder.addEntityDef(getEngineProfileEntity());
        this.archiveBuilder.addEntityDef(getEngineEntity());

        this.archiveBuilder.addClassificationDef(getWorkflowEngineClassification());
        this.archiveBuilder.addClassificationDef(getReportingEngineClassification());
        this.archiveBuilder.addClassificationDef(getAnalyticsEngineClassification());
        this.archiveBuilder.addClassificationDef(getDataMovementEngineClassification());
        this.archiveBuilder.addClassificationDef(getDataVirtualizationEngineClassification());
    }


    private EntityDef getEngineProfileEntity()
    {
        final String guid            = "81394f85-6008-465b-926e-b3fae4668937";
        final String name            = "EngineProfile";
        final String description     = "Descriptive details about a processing engine.";
        final String descriptionGUID = null;

        final String superTypeName = "ActorProfile";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getEngineEntity()
    {
        final String guid            = "3566527f-b1bd-4e7a-873e-a3e04d5f2a14";
        final String name            = "Engine";
        final String description     = "A programmable engine for running automated processes.";
        final String descriptionGUID = null;

        final String superTypeName = "SoftwareServerCapability";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private ClassificationDef getWorkflowEngineClassification()
    {
        final String guid            = "37a6d212-7c4a-4a82-b4e2-601d4358381c";
        final String name            = "WorkflowEngine";
        final String description     = "An engine capable of running a mixture of human and automated tasks as part of a workflow process.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Engine";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getReportingEngineClassification()
    {
        final String guid            = "e07eefaa-16e0-46cf-ad54-bed47fb15812";
        final String name            = "ReportingEngine";
        final String description     = "An engine capable of creating reports by combining information from multiple data sets.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Engine";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getAnalyticsEngineClassification()
    {
        final String guid            = "1a0dc6f6-7980-42f5-98bd-51e56543a07e";
        final String name            = "AnalyticsEngine";
        final String description     = "An engine capable of running analytics models using data from one or more data sets.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Engine";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getDataMovementEngineClassification()
    {
        final String guid            = "d2ed6621-9d99-4fe8-843a-b28d816cf888";
        final String name            = "DataMovementEngine";
        final String description     = "An engine capable of copying data from one data store to another.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Engine";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getDataVirtualizationEngineClassification()
    {
        final String guid            = "03e25cd0-03d7-4d96-b28b-eed671824ed6";
        final String name            = "DataVirtualizationEngine";
        final String description     = "An engine capable of creating new data sets by dynamically combining data from one or more data stores or data sets.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Engine";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0260Transformations()
    {
        /* placeholder */
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    private void add0265AnalyticsAssets()
    {
        /* placeholder */
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    private void add0270IoTAssets()
    {
        /* placeholder */
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0280ModelAssets()
    {
        /* placeholder */
    }


    /*
     * ========================================
     * AREA 3 - glossary
     */

    /**
     * Area 3 covers semantic definitions.
     */
    private void addArea3Types()
    {
        this.add0310Glossary();
        this.add0320CategoryHierarchy();
        this.add0330Terms();
        this.add0340Dictionary();
        this.add0350RelatedTerms();
        this.add0360Contexts();
        this.add0370SemanticAssignment();
        this.add0380SpineObjects();
        this.add0385ControlledGlossaryDevelopment();
        this.add0390GlossaryProject();
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0310 Glossary covers the top-level glossary object that organizes glossary terms.
     */
    private void add0310Glossary()
    {
        this.archiveBuilder.addEntityDef(getGlossaryEntity());
        this.archiveBuilder.addEntityDef(getExternalGlossaryLinkEntity());

        this.archiveBuilder.addRelationshipDef(getExternalSourcedGlossaryRelationship());

        this.archiveBuilder.addClassificationDef(getTaxonomyClassification());
        this.archiveBuilder.addClassificationDef(getCanonicalVocabularyClassification());
    }


    private EntityDef getGlossaryEntity()
    {
        final String guid            = "36f66863-9726-4b41-97ee-714fd0dc6fe4";
        final String name            = "Glossary";
        final String description     = "A collection of related glossary terms.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for the glossary, suitable for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the glossary.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "language";
        final String attribute3Description     = "Natural language used in the glossary.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "usage";
        final String attribute4Description     = "Guidance on the usage of this glossary content.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getExternalGlossaryLinkEntity()
    {
        final String guid            = "183d2935-a950-4d74-b246-eac3664b5a9d";
        final String name            = "ExternalGlossaryLink";
        final String description     = "The location of a glossary stored outside of the open metadata ecosystem.";
        final String descriptionGUID = null;

        final String superTypeName = "ExternalReference";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getExternalSourcedGlossaryRelationship()
    {
        final String guid            = "7786a39c-436b-4538-acc7-d595b5856add";
        final String name            = "ExternallySourcedGlossary";
        final String description     = "Link between an open metadata glossary and a related glossary stored outside of the open metadata ecosystem.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Glossary";
        final String               end1AttributeName            = "localGlossary";
        final String               end1AttributeDescription     = "Local glossary that relates to this external glossary.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalGlossaryLink";
        final String               end2AttributeName            = "externalGlossaryLink";
        final String               end2AttributeDescription     = "Link to a related external glossary.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private ClassificationDef getTaxonomyClassification()
    {
        final String guid            = "37116c51-e6c9-4c37-942e-35d48c8c69a0";
        final String name            = "Taxonomy";
        final String description     = "Identifies a glossary that includes a taxonomy.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Glossary";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "organizingPrinciple";
        final String attribute1Description     = "Characteristics that influence the organization of the taxonomy.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCanonicalVocabularyClassification()
    {
        final String guid            = "33ad3da2-0910-47be-83f1-daee018a4c05";
        final String name            = "CanonicalVocabulary";
        final String description     = "Identifies a glossary that contains unique terms.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Glossary";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "scope";
        final String attribute1Description     = "Scope of influence for this canonical glossary.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0320 Category Hierarchy adds categories to the glossary that allow terms to be defined in taxonomies
     * or hierarchies of folders.
     */
    private void add0320CategoryHierarchy()
    {
        this.archiveBuilder.addEntityDef(getGlossaryCategoryEntity());

        this.archiveBuilder.addRelationshipDef(getCategoryAnchorRelationship());
        this.archiveBuilder.addRelationshipDef(getCategoryHierarchyLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getLibraryCategoryReferenceRelationship());

        this.archiveBuilder.addClassificationDef(getSubjectAreaClassification());
    }


    private EntityDef getGlossaryCategoryEntity()
    {
        final String guid            = "e507485b-9b5a-44c9-8a28-6967f7ff3672";
        final String name            = "GlossaryCategory";
        final String description     = "A collection of related glossary terms.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for the glossary category, suitable for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the glossary category.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getCategoryAnchorRelationship()
    {
        final String guid            = "c628938e-815e-47db-8d1c-59bb2e84e028";
        final String name            = "CategoryAnchor";
        final String description     = "Connects a glossary category with its owning glossary.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Glossary";
        final String               end1AttributeName            = "anchor";
        final String               end1AttributeDescription     = "Owning glossary for this category.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryCategory";
        final String               end2AttributeName            = "categories";
        final String               end2AttributeDescription     = "Categories owned by this glossary.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getCategoryHierarchyLinkRelationship()
    {
        final String guid            = "71e4b6fb-3412-4193-aff3-a16eccd87e8e";
        final String name            = "CategoryHierarchyLink";
        final String description     = "Relationship between two glossary categories used to create nested categories.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryCategory";
        final String               end1AttributeName            = "superCategory";
        final String               end1AttributeDescription     = "Identifies the parent category.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryCategory";
        final String               end2AttributeName            = "subcategories";
        final String               end2AttributeDescription     = "Glossary categories nested inside this category.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getLibraryCategoryReferenceRelationship()
    {
        final String guid            = "3da21cc9-3cdc-4d87-89b5-c501740f00b2";
        final String name            = "LibraryCategoryReference";
        final String description     = "Links a glossary category to a corresponding category in an external glossary.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryCategory";
        final String               end1AttributeName            = "localCategories";
        final String               end1AttributeDescription     = "Related local glossary categories.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalGlossaryLink";
        final String               end2AttributeName            = "externalGlossaryLinks";
        final String               end2AttributeDescription     = "Links to related external glossaries.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "identifier";
        final String attribute1Description     = "Identifier of the corresponding element from the external glossary.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the corresponding element from the external glossary.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person who established the link to the external glossary.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "lastVerified";
        final String attribute4Description     = "Date when this reference was last checked.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getSubjectAreaClassification()
    {
        final String guid            = "480e6993-35c5-433a-b50b-0f5c4063fb5d";
        final String name            = "SubjectArea";
        final String description     = "Identifies a glossary category as a subject area.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryCategory";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the subject area.";
        final String attribute1DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0330 Terms brings in hte glossary term that captures a single semantic meaning.
     */
    private void add0330Terms()
    {
        this.archiveBuilder.addEnumDef(getTermRelationshipStatusEnum());

        this.archiveBuilder.addEntityDef(getGlossaryTermEntity());

        this.archiveBuilder.addRelationshipDef(getTermAnchorRelationship());
        this.archiveBuilder.addRelationshipDef(getTermCategorizationRelationship());
        this.archiveBuilder.addRelationshipDef(getLibraryTermReferenceRelationship());
    }

    private EnumDef getTermRelationshipStatusEnum()
    {
        final String guid            = "42282652-7d60-435e-ad3e-7cfe5291bcc7";
        final String name            = "TermRelationshipStatus";
        final String description     = "Defines the confidence in the assigned relationship.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Draft";
        final String element1Description     = "The term relationship is in development.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Active";
        final String element2Description     = "The term relationship is approved and in use.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Deprecated";
        final String element3Description     = "The term relationship should no longer be used.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Obsolete";
        final String element4Description     = "The term relationship must no longer be used.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another term relationship status.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getGlossaryTermEntity()
    {
        final String guid            = "0db3e6ec-f5ef-4d75-ae38-b7ee6fd6ec0a";
        final String name            = "GlossaryTerm";
        final String description     = "A semantic description of something, such as a concept, object, asset, technology, role or group.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name for the glossary term, suitable for reports and user interfaces.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "summary";
        final String attribute2Description     = "Short description of the glossary term.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "description";
        final String attribute3Description     = "Full description of the glossary term.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "examples";
        final String attribute4Description     = "Examples of this glossary term in use.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "abbreviation";
        final String attribute5Description     = "How this glossary term is abbreviated.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "usage";
        final String attribute6Description     = "Further guidance on the use of this glossary term.";
        final String attribute6DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getTermAnchorRelationship()
    {
        final String guid            = "1d43d661-bdc7-4a91-a996-3239b8f82e56";
        final String name            = "TermAnchor";
        final String description     = "Links a term to its owning glossary.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.COMPOSITION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Glossary";
        final String               end1AttributeName            = "anchor";
        final String               end1AttributeDescription     = "Owning glossary.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "terms";
        final String               end2AttributeDescription     = "Terms owned by this glossary.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getTermCategorizationRelationship()
    {
        final String guid            = "696a81f5-ac60-46c7-b9fd-6979a1e7ad27";
        final String name            = "TermCategorization";
        final String description     = "Links a glossary term into a glossary category.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryCategory";
        final String               end1AttributeName            = "categories";
        final String               end1AttributeDescription     = "Glossary categories that this term is linked to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "terms";
        final String               end2AttributeDescription     = "Glossary terms linked to this category.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Explanation of why this term is in this categorization.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "status";
        final String attribute2Description     = "Status of the relationship.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getLibraryTermReferenceRelationship()
    {
        final String guid            = "38c346e4-ddd2-42ef-b4aa-55d53c078d22";
        final String name            = "IsTaxonomy";
        final String description     = "Links a glossary term to a glossary term in an external glossary.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "localTerms";
        final String               end1AttributeDescription     = "Related local glossary terms.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ExternalGlossaryLink";
        final String               end2AttributeName            = "externalGlossaryLinks";
        final String               end2AttributeDescription     = "Links to related external glossaries.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "identifier";
        final String attribute1Description     = "Identifier of the corresponding element from the external glossary.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the corresponding element from the external glossary.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person who established the link to the external glossary.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "lastVerified";
        final String attribute4Description     = "Date when this reference was last checked.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0340 Dictionary provides classifications for a term that define what type of term it is and
     * how it intended to be used.
     */
    private void add0340Dictionary()
    {
        this.archiveBuilder.addEnumDef(getActivityTypeEnum());

        this.archiveBuilder.addClassificationDef(getActivityDescriptionClassification());
        this.archiveBuilder.addClassificationDef(getAbstractConceptClassification());
        this.archiveBuilder.addClassificationDef(getDataValueClassification());
    }


    private EnumDef getActivityTypeEnum()
    {
        final String guid            = "af7e403d-9865-4ebb-8c1a-1fd57b4f4bca";
        final String name            = "ActivityType";
        final String description     = "Different types of activities.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Operation";
        final String element1Description     = "Normal processing.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Action";
        final String element2Description     = "A requested of required change.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Task";
        final String element3Description     = "A piece of work for a person, organization or engine.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Process";
        final String element4Description     = "A sequence of tasks.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 3;
        final String element5Value           = "Project";
        final String element5Description     = "An organized activity to achieve a specific goal.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another type of activity.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private ClassificationDef getActivityDescriptionClassification()
    {
        final String guid            = "317f0e52-1548-41e6-b90c-6ae5e6c53fed";
        final String name            = "ActivityDescription";
        final String description     = "Identifies that this glossary term describes an activity.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "activityType";
        final String attribute1Description     = "Classification of the activity.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("ActivityType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getAbstractConceptClassification()
    {
        final String guid            = "9d725a07-4abf-4939-a268-419d200b69c2";
        final String name            = "AbstractConcept";
        final String description     = "Identifies that this glossary term describes an abstract concept.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getDataValueClassification()
    {
        final String guid            = "ab253e31-3d8a-45a7-8592-24329a189b9e";
        final String name            = "DataValue";
        final String description     = "Identifies that this glossary term describes a data value.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0350 Related Terms provides a selection of semantic relationships
     */
    private void add0350RelatedTerms()
    {
        this.archiveBuilder.addRelationshipDef(getRelatedTermRelationship());
        this.archiveBuilder.addRelationshipDef(getSynonymRelationship());
        this.archiveBuilder.addRelationshipDef(getAntonymRelationship());
        this.archiveBuilder.addRelationshipDef(getPreferredTermRelationship());
        this.archiveBuilder.addRelationshipDef(getReplacementTermRelationship());
        this.archiveBuilder.addRelationshipDef(getTranslationRelationship());
        this.archiveBuilder.addRelationshipDef(getISARelationshipRelationship());
        this.archiveBuilder.addRelationshipDef(getValidValueRelationship());
    }


    private RelationshipDef getRelatedTermRelationship()
    {
        final String guid            = "b1161696-e563-4cf9-9fd9-c0c76e47d063";
        final String name            = "RelatedTerm";
        final String description     = "Link between similar glossary terms.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "seeAlso";
        final String               end1AttributeDescription     = "Related glossary terms.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "seeAlso";
        final String               end2AttributeDescription     = "Related glossary terms.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getSynonymRelationship()
    {
        final String guid            = "74f4094d-dba2-4ad9-874e-d422b69947e2";
        final String name            = "Synonym";
        final String description     = "Link between glossary terms that have the same meaning.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "synonyms";
        final String               end1AttributeDescription     = "Glossary terms with the same meaning.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "synonyms";
        final String               end2AttributeDescription     = "Glossary terms with the same meaning.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getAntonymRelationship()
    {
        final String guid            = "ea5e126a-a8fa-4a43-bcfa-309a98aa0185";
        final String name            = "Antonym";
        final String description     = "Link between glossary terms that have the opposite meaning.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "antonyms";
        final String               end1AttributeDescription     = "Glossary terms with the opposite meaning.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "antonyms";
        final String               end2AttributeDescription     = "Glossary terms with the opposite meaning.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getPreferredTermRelationship()
    {
        final String guid            = "8ac8f9de-9cdd-4103-8a33-4cb204b78c2a";
        final String name            = "PreferredTerm";
        final String description     = "Link to an alternative term that the organization prefer is used.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "alternateTerms";
        final String               end1AttributeDescription     = "Alternative glossary terms.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "preferredTerms";
        final String               end2AttributeDescription     = "Related glossary terms.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getReplacementTermRelationship()
    {
        final String guid            = "3bac5f35-328b-4bbd-bfc9-3b3c9ba5e0ed";
        final String name            = "ReplacementTerm";
        final String description     = "Link to a glossary term that is replacing an obsolete glossary term.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "replacedTerms";
        final String               end1AttributeDescription     = "Replaced glossary terms.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "replacementTerms";
        final String               end2AttributeDescription     = "Replacement glossary terms.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getTranslationRelationship()
    {
        final String guid            = "6ae42e95-efc5-4256-bfa8-801140a29d2a";
        final String name            = "Translation";
        final String description     = "Link between glossary terms that provide different natural language translation of the same concept.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "translations";
        final String               end1AttributeDescription     = "Translations of glossary term.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "translations";
        final String               end2AttributeDescription     = "Translations of glossary term.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getISARelationshipRelationship()
    {
        final String guid            = "50fab7c7-68bc-452f-b8eb-ec76829cac85";
        final String name            = "ISARelationship";
        final String description     = "Link between a more general glossary term and a more specific definition.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "classifies";
        final String               end1AttributeDescription     = "More specific glossary terms.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "isA";
        final String               end2AttributeDescription     = "More general glossary terms.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getValidValueRelationship()
    {
        final String guid            = "707a156b-e579-4482-89a5-de5889da1971";
        final String name            = "ValidValue";
        final String description     = "Link between glossary terms where one defines one of the data values for the another.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "validValueFor";
        final String               end1AttributeDescription     = "Glossary terms for data items that can be set to this value.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "validValues";
        final String               end2AttributeDescription     = "Glossary terms for data values that can be used with data items represented by this glossary term.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0360 Contexts allows Glossary Terms to linked to specific contexts (also defined with Glossary Terms).
     */
    private void add0360Contexts()
    {
        this.archiveBuilder.addRelationshipDef(getUsedInContextRelationship());

        this.archiveBuilder.addClassificationDef(getContextDefinitionClassification());
    }


    private RelationshipDef getUsedInContextRelationship()
    {
        final String guid            = "2dc524d2-e29f-4186-9081-72ea956c75de";
        final String name            = "UsedInContext";
        final String description     = "Link between glossary terms where on describes the context where the other one is valid to use.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "contextRelevantTerms";
        final String               end1AttributeDescription     = "Glossary terms used in this specific context.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "usedInContexts";
        final String               end2AttributeDescription     = "Glossary terms describing the contexts where this term is used.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "An expression that explains the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getContextDefinitionClassification()
    {
        final String guid            = "54f9f41a-3871-4650-825d-59a41de01330";
        final String name            = "ContextDefinition";
        final String description     = "Identifies a glossary term that describes a context where processing or decisions occur.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description for how the context is used.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "scope";
        final String attribute2Description     = "Scope of influence of the context.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0370 Semantic Assignment defines a relationship between a Glossary Term and an Asset
     */
    private void add0370SemanticAssignment()
    {
        this.archiveBuilder.addEnumDef(getTermAssignmentStatusEnum());

        this.archiveBuilder.addRelationshipDef(getSemanticAssignmentRelationship());
    }


    private EnumDef getTermAssignmentStatusEnum()
    {
        final String guid            = "c8fe36ac-369f-4799-af75-46b9c1343ab3";
        final String name            = "TermAssignmentStatus";
        final String description     = "Defines the provenance and confidence of a term assignment.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Discovered";
        final String element1Description     = "The term assignment was discovered by an automated process.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Proposed";
        final String element2Description     = "The term assignment was proposed by a subject matter expert.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Imported";
        final String element3Description     = "The term assignment was imported from another metadata system.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Validated";
        final String element4Description     = "The term assignment has been validated and approved by a subject matter expert.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Deprecated";
        final String element5Description     = "The term assignment should no longer be used.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "Obsolete";
        final String element6Description     = "The term assignment must no longer be used.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another term assignment status.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private RelationshipDef getSemanticAssignmentRelationship()
    {
        final String guid            = "e6670973-645f-441a-bec7-6f5570345b92";
        final String name            = "SemanticAssignment";
        final String description     = "Links a glossary term to another element such as an asset or schema element to define its meaning.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "assignedElements";
        final String               end1AttributeDescription     = "Elements identified as managing data that has the same meaning as this glossary term.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "meaning";
        final String               end2AttributeDescription     = "Semantic definition for this element.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "expression";
        final String attribute2Description     = "Expression describing the relationship.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "confidence";
        final String attribute4Description     = "Level of confidence in the correctness of the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "steward";
        final String attribute5Description     = "Person responsible for the relationship.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "source";
        final String attribute6Description     = "Person, organization or automated process that created the relationship.";
        final String attribute6DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0380 Spine Objects enables relationships to be established between objects and their attributes.
     */
    private void add0380SpineObjects()
    {
        this.archiveBuilder.addRelationshipDef(getTermHASARelationshipRelationship());
        this.archiveBuilder.addRelationshipDef(getTermISATYPEOFRelationshipRelationship());
        this.archiveBuilder.addRelationshipDef(getTermTYPEDBYRelationshipRelationship());

        this.archiveBuilder.addClassificationDef(getSpineObjectClassification());
        this.archiveBuilder.addClassificationDef(getSpineAttributeClassification());
        this.archiveBuilder.addClassificationDef(getObjectIdentifierClassification());
    }


    private RelationshipDef getTermHASARelationshipRelationship()
    {
        final String guid            = "d67f16d1-5348-419e-ba38-b0bb6fe4ad6c";
        final String name            = "TermHASARelationship";
        final String description     = "Defines the relationship between a spine object and a spine attribute.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "objects";
        final String               end1AttributeDescription     = "Objects where this attribute may occur.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "attributes";
        final String               end2AttributeDescription     = "Typical attributes for this object.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getTermISATYPEOFRelationshipRelationship()
    {
        final String guid            = "d5d588c3-46c9-420c-adff-6031802a7e51";
        final String name            = "TermISATypeOFRelationship";
        final String description     = "Defines an inheritance relationship between two spine objects.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "supertypes";
        final String               end1AttributeDescription     = "Supertypes for this object.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "subtypes";
        final String               end2AttributeDescription     = "Subtypes for this object.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getTermTYPEDBYRelationshipRelationship()
    {
        final String guid            = "669e8aa4-c671-4ee7-8d03-f37d09b9d006";
        final String name            = "TermTYPEDBYRelationship";
        final String description     = "Defines the relationship between a spine attribute and its type.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GlossaryTerm";
        final String               end1AttributeName            = "attributesTypedBy";
        final String               end1AttributeDescription     = "Attributes of this type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GlossaryTerm";
        final String               end2AttributeName            = "types";
        final String               end2AttributeDescription     = "Types for this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;
        final String attribute3Name            = "status";
        final String attribute3Description     = "The status of or confidence in the relationship.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "steward";
        final String attribute4Description     = "Person responsible for the relationship.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "source";
        final String attribute5Description     = "Person, organization or automated process that created the relationship.";
        final String attribute5DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("TermRelationshipStatus", attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getSpineObjectClassification()
    {
        final String guid            = "a41ee152-de1e-4533-8535-2f8b37897cac";
        final String name            = "SpineObject";
        final String description     = "Identifies a glossary term that describes a type of spine object.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getSpineAttributeClassification()
    {
        final String guid            = "ccb749ba-34ec-4f71-8755-4d8b383c34c3";
        final String name            = "SpineAttribute";
        final String description     = "Identifies a glossary term that describes an attribute of a spine object.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getObjectIdentifierClassification()
    {
        final String guid            = "3d1e4389-27de-44fa-8df4-d57bfaf809ea";
        final String name            = "ObjectIdentifier";
        final String description     = "Identifies a glossary term that describes an attribute that can be used to identify an instance.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0385 ControlledGlossaryDevelopment covers glossary term objects that are created and
     * maintained through a workflow process.
     */
    private void add0385ControlledGlossaryDevelopment()
    {
        this.archiveBuilder.addEntityDef(getControlledGlossaryTermEntity());
    }


    private EntityDef getControlledGlossaryTermEntity()
    {
        final String guid            = "c04e29b2-2d66-48fc-a20d-e59895de6040";
        final String name            = "ControlledGlossaryTerm";
        final String description     = "Defines a glossary term that is developed through a controlled workflow.";
        final String descriptionGUID = null;

        final String superTypeName = "GlossaryTerm";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0390 Glossary Project provides a classification for a project to say it is updating glossary terms.
     */
    private void add0390GlossaryProject()
    {
        this.archiveBuilder.addClassificationDef(getGlossaryProjectClassification());
    }


    private ClassificationDef getGlossaryProjectClassification()
    {
        final String guid            = "43be51a9-2d19-4044-b399-3ba36af10929";
        final String name            = "GlossaryProject";
        final String description     = "Identifies a project that is defining new glossary terms and categories or maintaining an existing glossary.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Project";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * ========================================
     * AREA 4 - governance
     */

    /**
     * Area 4 models cover the governance entities and relationships.
     */
    private void addArea4Types()
    {
        this.add0401GovernanceDefinitions();
        this.add0405GovernanceDrivers();
        this.add0415GovernanceResponses();
        this.add0417GovernanceProject();
        this.add0420GovernanceControls();
        this.add0422GovernanceActionClassifications();
        this.add0424GovernanceZones();
        this.add0430TechnicalControls();
        this.add0435GovernanceRules();
        this.add0438NamingStandards();
        this.add0440OrganizationalControls();
        this.add0442ProjectCharter();
        this.add0445GovernanceRoles();
        this.add0447GovernanceProcesses();
        this.add0450GovernanceRollout();
        this.add0452GovernanceDaemons();
        this.add0455ExceptionManagement();
        this.add0460GovernanceActions();
        this.add0480RightsManagement();
        this.add0481Licenses();
        this.add0482Certifications();
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0401 The definitions that control the governance of data assets are authored in the metadata repository.
     * They are referenceable and they make use of external references for more information.
     */
    private void add0401GovernanceDefinitions()
    {
        this.archiveBuilder.addEnumDef(getGovernanceDefinitionStatusEnum());

        this.archiveBuilder.addEntityDef(getGovernanceDefinitionEntity());
    }


    private EnumDef getGovernanceDefinitionStatusEnum()
    {
        final String guid            = "baa31998-f3cb-47b0-9123-674a701e87bc";
        final String name            = "GovernanceDefinitionStatus";
        final String description     = "Defines the status values of a governance definition.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Proposed";
        final String element1Description     = "The governance definition is under development.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Active";
        final String element2Description     = "The governance definition is approved and in use.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Deprecated";
        final String element3Description     = "The governance definition has been replaced.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Obsolete";
        final String element4Description     = "The governance definition must not be used any more.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another governance definition status.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getGovernanceDefinitionEntity()
    {
        final String guid            = "578a3500-9ad3-45fe-8ada-e4e9572c37c8";
        final String name            = "GovernanceDefinition";
        final String description     = "Defines an aspect of the governance program.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "title";
        final String attribute1Description     = "Title describing the governance definition.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "summary";
        final String attribute2Description     = "Short summary of the governance definition.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "description";
        final String attribute3Description     = "Detailed description of the governance definition.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "scope";
        final String attribute4Description     = "Scope of impact for this governance definition.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "status";
        final String attribute5Description     = "Current status of this governance definition.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "priority";
        final String attribute6Description     = "Relative importance of this governance definition compared to its peers.";
        final String attribute6DescriptionGUID = null;
        final String attribute7Name            = "implications";
        final String attribute7Description     = "Impact on the organization, people and services when adopting the recommendation in this governance definition.";
        final String attribute7DescriptionGUID = null;
        final String attribute8Name            = "outcomes";
        final String attribute8Description     = "Expected outcomes.";
        final String attribute8DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("GovernanceDefinitionStatus", attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getArrayStringTypeDefAttribute(attribute7Name, attribute7Description, attribute7DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getArrayStringTypeDefAttribute(attribute8Name, attribute8Description, attribute8DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0405 Governance Drivers defines the factors that drive the governance program.
     */
    private void add0405GovernanceDrivers()
    {
        this.archiveBuilder.addEntityDef(getGovernanceDriverEntity());
        this.archiveBuilder.addEntityDef(getDataStrategyEntity());
        this.archiveBuilder.addEntityDef(getRegulationEntity());
    }


    private EntityDef getGovernanceDriverEntity()
    {
        final String guid            = "c403c109-7b6b-48cd-8eee-df445b258b33";
        final String name            = "GovernanceDriver";
        final String description     = "Defines a reason for having the governance program.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDefinition";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getDataStrategyEntity()
    {
        final String guid            = "3c34f121-07a6-4e95-a07d-9b0ef17b7bbf";
        final String name            = "DataStrategy";
        final String description     = "Defines how data and the supporting capabilities are supporting the business strategy.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDriver";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "businessImperative";
        final String attribute1Description     = "Goal or required outcome from the business strategy that is supported by the data strategy.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getRegulationEntity()
    {
        final String guid            = "e3c4293d-8846-4500-b0c0-197d73aba8b0";
        final String name            = "Regulation";
        final String description     = "Identifies a regulation related to data that must be supported.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDriver";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "jurisdiction";
        final String attribute1Description     = "Issuing authority for the regulation.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0415 Governance Responses lay out the approaches, principles and obligations that follow from the
     * governance drivers.
     */
    private void add0415GovernanceResponses()
    {
        this.archiveBuilder.addEntityDef(getGovernancePolicyEntity());
        this.archiveBuilder.addEntityDef(getGovernancePrincipleEntity());
        this.archiveBuilder.addEntityDef(getGovernanceObligationEntity());
        this.archiveBuilder.addEntityDef(getGovernanceApproachEntity());

        this.archiveBuilder.addRelationshipDef(getGovernancePolicyLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceResponseRelationship());
    }


    private EntityDef getGovernancePolicyEntity()
    {
        final String guid            = "a7defa41-9cfa-4be5-9059-359022bb016d";
        final String name            = "GovernancePolicy";
        final String description     = "Defines a goal or outcome expected from the organization.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDefinition";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGovernancePrincipleEntity()
    {
        final String guid            = "3b7d1325-ec2c-44cb-8db0-ce207beb78cf";
        final String name            = "GovernancePrinciple";
        final String description     = "Defines a principle related to how data is managed or used that the organization should ensure remains true.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernancePolicy";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGovernanceObligationEntity()
    {
        final String guid            = "0cec20d3-aa29-41b7-96ea-1c544ed32537";
        final String name            = "GovernanceObligation";
        final String description     = "Defines a capability, rule or action that is required by a regulation or external party.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernancePolicy";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGovernanceApproachEntity()
    {
        final String guid            = "2d03ec9d-bd6b-4be9-8e17-95a7ecdbaa67";
        final String name            = "GovernanceApproach";
        final String description     = "Defines a preferred approach to managing or using data.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernancePolicy";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getGovernancePolicyLinkRelationship()
    {
        final String guid            = "0c42c999-4cac-4da4-afab-0e381f3a818e";
        final String name            = "GovernancePolicyLink";
        final String description     = "Links related governance policies together.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernancePolicy";
        final String               end1AttributeName            = "linkingPolicies";
        final String               end1AttributeDescription     = "Policies that are dependent on this policy.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernancePolicy";
        final String               end2AttributeName            = "linkedPolicies";
        final String               end2AttributeDescription     = "Policies that further define aspects of this policy.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getGovernanceResponseRelationship()
    {
        final String guid            = "8845990e-7fd9-4b79-a19d-6c4730dadd6b";
        final String name            = "GovernanceResponse";
        final String description     = "Links a governance policy to a governance driver that it is supporting.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceDriver";
        final String               end1AttributeName            = "drivers";
        final String               end1AttributeDescription     = "Drivers that justify this policy.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernancePolicy";
        final String               end2AttributeName            = "policies";
        final String               end2AttributeDescription     = "Governance policies that support this governance driver.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "rationale";
        final String attribute1Description     = "Describes the reasoning for defining the policy in support of the driver.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);


        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0417 Governance Project provides a classification for a project
     */
    private void add0417GovernanceProject()
    {
        this.archiveBuilder.addClassificationDef(getGovernanceProjectClassification());
    }


    private ClassificationDef getGovernanceProjectClassification()
    {
        final String guid            = "37142317-4125-4046-9514-71dc5031563f";
        final String name            = "GovernanceProject";
        final String description     = "Identifies that a project is rolling out capability to support the governance program.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Project";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0420 Governance Controls define the implementation of the governance policies
     */
    private void add0420GovernanceControls()
    {
        this.archiveBuilder.addEntityDef(getGovernanceControlEntity());
        this.archiveBuilder.addEntityDef(getTechnicalControlEntity());
        this.archiveBuilder.addEntityDef(getOrganizationalControlEntity());

        this.archiveBuilder.addRelationshipDef(getGovernanceImplementationRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceControlLinkRelationship());
    }


    private EntityDef getGovernanceControlEntity()
    {
        final String guid            = "c794985e-a10b-4b6c-9dc2-6b2e0a2901d3";
        final String name            = "GovernanceControl";
        final String description     = "An implementation of a governance capability.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDefinition";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "implementationDescription";
        final String attribute1Description     = "Description of how this governance control should be implemented.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getTechnicalControlEntity()
    {
        final String guid            = "d8f6eb5b-36f0-49bd-9b25-bf16f370d1ec";
        final String name            = "TechnicalControl";
        final String description     = "A governance control that is implemented using technology.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getOrganizationalControlEntity()
    {
        final String guid            = "befa1458-79b8-446a-b813-536700e60fa8";
        final String name            = "OrganizationalControl";
        final String description     = "A governance control that is implemented using organization structure, training, roles manual procedures and reviews.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getGovernanceImplementationRelationship()
    {
        final String guid            = "787eaf46-7cf2-4096-8d6e-671a0819d57e";
        final String name            = "GovernanceImplementation";
        final String description     = "A link between a governance control and the governance driver it is implementing.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernancePolicy";
        final String               end1AttributeName            = "policies";
        final String               end1AttributeDescription     = "The policies that are supported by this control.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernanceControl";
        final String               end2AttributeName            = "implementations";
        final String               end2AttributeDescription     = "The governance controls that implement this policy.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "rationale";
        final String attribute1Description     = "The reasons for implementing the policy using this control.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getGovernanceControlLinkRelationship()
    {
        final String guid            = "806933fb-7925-439b-9876-922a960d2ba1";
        final String name            = "GovernanceControlLink";
        final String description     = "A link between two related governance controls.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceControl";
        final String               end1AttributeName            = "linkingControls";
        final String               end1AttributeDescription     = "Governance controls that ate dependent on this control.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernanceControl";
        final String               end2AttributeName            = "linkedControls";
        final String               end2AttributeDescription     = "Governance controls that support the implementation of this control.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the relationship.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0422 Governance Action Classifications provide the key classification that drive information governance.
     */
    private void add0422GovernanceActionClassifications()
    {
        this.archiveBuilder.addEnumDef(getGovernanceClassificationStatusEnum());
        this.archiveBuilder.addEnumDef(getConfidentialityLevelEnum());
        this.archiveBuilder.addEnumDef(getConfidenceLevelEnum());
        this.archiveBuilder.addEnumDef(getRetentionBasisEnum());
        this.archiveBuilder.addEnumDef(getCriticalityLevelEnum());

        this.archiveBuilder.addClassificationDef(getConfidentialityClassification());
        this.archiveBuilder.addClassificationDef(getConfidenceClassification());
        this.archiveBuilder.addClassificationDef(getRetentionClassification());
        this.archiveBuilder.addClassificationDef(getCriticalityClassification());
    }


    private EnumDef getGovernanceClassificationStatusEnum()
    {
        final String guid            = "cc540586-ac7c-41ba-8cc1-4da694a6a8e4";
        final String name            = "GovernanceClassificationStatus";
        final String description     = "Defines the status values of a governance action classification.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Discovered";
        final String element1Description     = "The classification assignment was discovered by an automated process.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Proposed";
        final String element2Description     = "The classification assignment was proposed by a subject matter expert.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Imported";
        final String element3Description     = "The classification assignment was imported from another metadata system.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Validated";
        final String element4Description     = "The classification assignment has been validated and approved by a subject matter expert.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Deprecated";
        final String element5Description     = "The classification assignment should no longer be used.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "Obsolete";
        final String element6Description     = "The classification assignment must no longer be used.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another classification assignment status.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getConfidentialityLevelEnum()
    {
        final String guid            = "ecb48ca2-4d29-4de9-99a1-bc4db9816d68";
        final String name            = "ConfidentialityLevel";
        final String description     = "Defines how confidential a data item is.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Unclassified";
        final String element1Description     = "The data is public information.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);
        enumDef.setDefaultValue(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Internal";
        final String element2Description     = "The data should not be exposed outside of this organization.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Confidential";
        final String element3Description     = "The data should be protected and only shared with people with a need to see it.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Sensitive";
        final String element4Description     = "The data is sensitive and inappropriate use may adversely impact the data subject.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Restricted";
        final String element5Description     = "The data is very valuable and must be restricted to a very small number of people.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another confidentially level.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getConfidenceLevelEnum()
    {
        final String guid            = "ae846797-d88a-4421-ad9a-318bf7c1fe6f";
        final String name            = "ConfidenceLevel";
        final String description     = "Defines the level of confidence to place in the accuracy of a data item.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Unclassified";
        final String element1Description     = "There is no assessment of the confidence level of this data.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);
        enumDef.setDefaultValue(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "AdHoc";
        final String element2Description     = "The data comes from an ad hoc process.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Transactional";
        final String element3Description     = "The data comes from a transactional system so it may have a narrow scope.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Authoritative";
        final String element4Description     = "The data comes from an authoritative source.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Derived";
        final String element5Description     = "The data is derived from other data through an analytical process.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "Obsolete";
        final String element6Description     = "The data comes from an obsolete source and must no longer be used.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another confidence level.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getRetentionBasisEnum()
    {
        final String guid            = "de79bf78-ecb0-4fd0-978f-ecc2cb4ff6c7";
        final String name            = "RetentionBasis";
        final String description     = "Defines the retention requirements associated with a data item.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Unclassified";
        final String element1Description     = "There is no assessment of the retention requirements for this data.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Temporary";
        final String element2Description     = "This data is temporary.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "ProjectLifetime";
        final String element3Description     = "The data is needed for the lifetime of the referenced project.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "TeamLifetime";
        final String element4Description     = "The data is needed for the lifetime of the referenced team.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "ContractLifetime";
        final String element5Description     = "The data is needed for the lifetime of the referenced contract.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "RegulatedLifetime";
        final String element6Description     = "The retention period for the data is defined by the referenced regulation.";
        final String element6DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element7Ordinal         = 6;
        final String element7Value           = "TimeBoxedLifetime";
        final String element7Description     = "The data is needed for the specified time.";
        final String element7DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element7Ordinal, element7Value, element7Description, element7DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another basis for determining the retention requirement.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EnumDef getCriticalityLevelEnum()
    {
        final String guid            = "22bcbf49-83e1-4432-b008-e09a8f842a1e";
        final String name            = "CriticalityLevel";
        final String description     = "Defines how important a data item is to the organization.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Unclassified";
        final String element1Description     = "There is no assessment of the criticality of this data.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);
        enumDef.setDefaultValue(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Marginal";
        final String element2Description     = "The data is of minor importance to the organization.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Important";
        final String element3Description     = "The data is important to the running of the organization.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Critical";
        final String element4Description     = "The data is critical to the operation of the organization.";
        final String element4DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Catastrophic";
        final String element5Description     = "The data is so important that its loss is catastrophic putting the future of the organization in doubt.";
        final String element5DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another criticality level.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private ClassificationDef getConfidentialityClassification()
    {
        final String guid            = "742ddb7d-9a4a-4eb5-8ac2-1d69953bd2b6";
        final String name            = "Confidentiality";
        final String description     = "Defines the level of confidentiality of related data items.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "status";
        final String attribute1Description     = "Status of this classification.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "confidence";
        final String attribute2Description     = "Level of confidence in the classification (0=none -> 100=excellent).";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person responsible for maintaining this classification.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "source";
        final String attribute4Description     = "Source of the classification.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "notes";
        final String attribute5Description     = "Information relating to the classification.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "level";
        final String attribute6Description     = "Level of confidentiality.";
        final String attribute6DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("GovernanceClassificationStatus", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("ConfidentialityLevel", attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getConfidenceClassification()
    {
        final String guid            = "25d8f8d5-2998-4983-b9ef-265f58732965";
        final String name            = "Confidence";
        final String description     = "Defines the level of confidence that should be placed in the accuracy of related data items.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "status";
        final String attribute1Description     = "Status of this classification.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "confidence";
        final String attribute2Description     = "Level of confidence in the classification (0=none -> 100=excellent).";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person responsible for maintaining this classification.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "source";
        final String attribute4Description     = "Source of the classification.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "notes";
        final String attribute5Description     = "Information relating to the classification.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "level";
        final String attribute6Description     = "Level of confidence in the quality of this data.";
        final String attribute6DescriptionGUID = null;


        property = archiveHelper.getEnumTypeDefAttribute("GovernanceClassificationStatus", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("ConfidenceLevel", attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getRetentionClassification()
    {
        final String guid            = "83dbcdf2-9445-45d7-bb24-9fa661726553";
        final String name            = "Retention";
        final String description     = "Defines the retention requirements for related data items.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "status";
        final String attribute1Description     = "Status of this classification.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "confidence";
        final String attribute2Description     = "Level of confidence in the classification (0=none -> 100=excellent).";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person responsible for maintaining this classification.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "source";
        final String attribute4Description     = "Source of the classification.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "notes";
        final String attribute5Description     = "Information relating to the classification.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "basis";
        final String attribute6Description     = "Basis on which the retention period is defined.";
        final String attribute6DescriptionGUID = null;
        final String attribute7Name            = "associatedGUID";
        final String attribute7Description     = "Related entity used to determine the retention period.";
        final String attribute7DescriptionGUID = null;
        final String attribute8Name            = "archiveAfter";
        final String attribute8Description     = "Related entity used to determine the retention period.";
        final String attribute8DescriptionGUID = null;
        final String attribute9Name            = "deleteAfter";
        final String attribute9Description     = "Related entity used to determine the retention period.";
        final String attribute9DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("GovernanceClassificationStatus", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("RetentionBasis", attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute7Name, attribute7Description, attribute7DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute8Name, attribute8Description, attribute8DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute9Name, attribute9Description, attribute9DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getCriticalityClassification()
    {
        final String guid            = "d46d211a-bd22-40d5-b642-87b4954a167e";
        final String name            = "Criticality";
        final String description     = "Defines how critical the related data items are to the organization.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "status";
        final String attribute1Description     = "Status of this classification.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "confidence";
        final String attribute2Description     = "Level of confidence in the classification (0=none -> 100=excellent).";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "steward";
        final String attribute3Description     = "Person responsible for maintaining this classification.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "source";
        final String attribute4Description     = "Source of the classification.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "notes";
        final String attribute5Description     = "Information relating to the classification.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "level";
        final String attribute6Description     = "How critical is this data to the organization.";
        final String attribute6DescriptionGUID = null;


        property = archiveHelper.getEnumTypeDefAttribute("GovernanceClassificationStatus", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getEnumTypeDefAttribute("CriticalityLevel", attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0424 Governance Zones define the zones used to group assets according to their use.
     */
    private void add0424GovernanceZones()
    {
        this.archiveBuilder.addEntityDef(getGovernanceZoneEntity());

        this.archiveBuilder.addRelationshipDef(getZoneGovernanceRelationship());
        this.archiveBuilder.addRelationshipDef(getZoneMembershipRelationship());
    }


    private EntityDef getGovernanceZoneEntity()
    {
        final String guid            = "290a192b-42a7-449a-935a-269ca62cfdac";
        final String name            = "GovernanceZone";
        final String description     = "Defines a collection of assets that are suitable for a particular usage or are governed by a particular process.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name this zone for user interfaces and reports.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of this zone.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "criteria";
        final String attribute3Description     = "Definition of the types of assets that belong in this zone.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getZoneGovernanceRelationship()
    {
        final String guid            = "4c4d1d9c-a9fc-4305-8b71-4e891c0f9ae0";
        final String name            = "ZoneGovernance";
        final String description     = "Links a governance zone to a governance definition that applies to all of the members of the zone.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceZone";
        final String               end1AttributeName            = "governedZones";
        final String               end1AttributeDescription     = "The collections of assets governed by this definition.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernanceDefinition";
        final String               end2AttributeName            = "governedBy";
        final String               end2AttributeDescription     = "Governance definitions for this zone.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    private RelationshipDef getZoneMembershipRelationship()
    {
        final String guid            = "8866f770-3f8f-4062-ad2a-2841d4f7ceab";
        final String name            = "ZoneMembership";
        final String description     = "Defines that an asset is a member of a specific governance zone.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceZone";
        final String               end1AttributeName            = "zones";
        final String               end1AttributeDescription     = "The governance zones that his asset is a member of.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Asset";
        final String               end2AttributeName            = "zoneMembers";
        final String               end2AttributeDescription     = "The assets that are members for this zone.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);


        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0430 Technical Controls describe specific types of technical controls
     */
    private void add0430TechnicalControls()
    {
        this.archiveBuilder.addEntityDef(getGovernanceRuleEntity());
        this.archiveBuilder.addEntityDef(getGovernanceProcessEntity());

        this.archiveBuilder.addRelationshipDef(getGovernanceRuleImplementationRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceProcessImplementationRelationship());
    }


    private EntityDef getGovernanceRuleEntity()
    {
        final String guid            = "8f954380-12ce-4a2d-97c6-9ebe250fecf8";
        final String name            = "GovernanceRule";
        final String description     = "Technical control expressed as a logic expression.";
        final String descriptionGUID = null;

        final String superTypeName = "TechnicalControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGovernanceProcessEntity()
    {
        final String guid            = "b68b5d9d-6b79-4f3a-887f-ec0f81c54aea";
        final String name            = "GovernanceProcess";
        final String description     = "Technical control expressed as a sequence of tasks.";
        final String descriptionGUID = null;

        final String superTypeName = "TechnicalControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getGovernanceRuleImplementationRelationship()
    {
        final String guid            = "e701a5c8-c1ba-4b75-8257-e0a6569eda48";
        final String name            = "GovernanceRuleImplementation";
        final String description     = "Identifies the implementation of a governance rule.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceRule";
        final String               end1AttributeName            = "implementsGovernanceRules";
        final String               end1AttributeDescription     = "The rules that are implemented by this component.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SoftwareComponent";
        final String               end2AttributeName            = "implementations";
        final String               end2AttributeDescription     = "The software components that implement this governance rule.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Documents reasons for implementing the rule using this implementation.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getGovernanceProcessImplementationRelationship()
    {
        final String guid            = "a5a7b08a-73fd-4026-a9dd-d0fe55bea8a4";
        final String name            = "GovernanceProcessImplementation";
        final String description     = "Identifies the implementation of a governance process.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceProcess";
        final String               end1AttributeName            = "implementsGovernanceProcesses";
        final String               end1AttributeDescription     = "The processes that are implemented by this component.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Process";
        final String               end2AttributeName            = "implementations";
        final String               end2AttributeDescription     = "The processes that implement this governance process.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Documents reasons for implementing the process using this implementation.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0435 Governance Rules define details of a governance rule implementation.
     */
    private void add0435GovernanceRules()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0438 Naming Standards provides definitions for laying our naming standards for schemas and assets.
     */
    private void add0438NamingStandards()
    {
        this.archiveBuilder.addEntityDef(getNamingStandardRuleEntity());
        this.archiveBuilder.addEntityDef(getNamingStandardRuleSetEntity());

        this.archiveBuilder.addClassificationDef(getPrimeWordClassification());
        this.archiveBuilder.addClassificationDef(getClassWordClassification());
        this.archiveBuilder.addClassificationDef(getModifierClassification());
    }


    private EntityDef getNamingStandardRuleEntity()
    {
        final String guid            = "52505b06-98a5-481f-8a32-db9b02afabfc";
        final String name            = "NamingStandardRule";
        final String description     = "Describes a parsing rule used to create compliant names.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceRule";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "namePattern";
        final String attribute1Description     = "Format of the naming standard rule.";
        final String attribute1DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);


        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getNamingStandardRuleSetEntity()
    {
        final String guid            = "ba70f506-1f81-4890-bb4f-1cb1d99c939e";
        final String name            = "NamingStandardRuleSet";
        final String description     = "Describes a collection of related naming standard rules.";
        final String descriptionGUID = null;

        final String superTypeName = "Collection";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private ClassificationDef getPrimeWordClassification()
    {
        final String guid            = "3ea1ea66-8923-4662-8628-0bacef3e9c5f";
        final String name            = "PrimeWord";
        final String description     = "Describes a primary noun, used in naming standards.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getClassWordClassification()
    {
        final String guid            = "feac4bd9-37d9-4437-82f6-618ce3e2793e";
        final String name            = "ClassWord";
        final String description     = "Describes classifying or grouping noun, using in naming standards.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getModifierClassification()
    {
        final String guid            = "dfc70bed-7e8b-4060-910c-59c7473f23a3";
        final String name            = "NamingConventionRule";
        final String description     = "Describes modifying noun or adverb, used in naming standards.";
        final String descriptionGUID = null;

        final String linkedToEntity = "GlossaryTerm";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0440 Organizational Controls describe organizational roles and responsibilities.
     */
    private void add0440OrganizationalControls()
    {
        this.archiveBuilder.addEnumDef(getBusinessCapabilityTypeEnum());

        this.archiveBuilder.addEntityDef(getOrganizationEntity());
        this.archiveBuilder.addEntityDef(getBusinessCapabilityEntity());
        this.archiveBuilder.addEntityDef(getGovernanceResponsibilityEntity());
        this.archiveBuilder.addEntityDef(getGovernanceProcedureEntity());

        this.archiveBuilder.addRelationshipDef(getOrganizationCapabilityRelationship());
        this.archiveBuilder.addRelationshipDef(getResponsibilityStaffContactRelationship());
        this.archiveBuilder.addRelationshipDef(getBusinessCapabilityResponsibilityRelationship());
    }


    private EnumDef getBusinessCapabilityTypeEnum()
    {
        final String guid            = "fb7c40cf-8d95-48ff-ba8b-e22bff6f5a91";
        final String name            = "BusinessCapabilityType";
        final String description     = "Defines the type or category of business capability.";
        final String descriptionGUID = null;

        EnumDef enumDef = archiveHelper.getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs = new ArrayList<>();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Unclassified";
        final String element1Description     = "The business capability has not been classified.";
        final String element1DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);
        enumDef.setDefaultValue(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "BusinessService";
        final String element2Description     = "A functional business capability.";
        final String element2DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "BusinessArea";
        final String element3Description     = "A collection of related business services.";
        final String element3DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element99Ordinal         = 99;
        final String element99Value           = "Other";
        final String element99Description     = "Another governance definition status.";
        final String element99DescriptionGUID = null;

        elementDef = archiveHelper.getEnumElementDef(element99Ordinal, element99Value, element99Description, element99DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }


    private EntityDef getOrganizationEntity()
    {
        final String guid            = "50a61105-35be-4ee3-8b99-bdd958ed0685";
        final String name            = "Organization";
        final String description     = "Describes a specific organization.";
        final String descriptionGUID = null;

        final String superTypeName = "Team";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getBusinessCapabilityEntity()
    {
        final String guid            = "7cc6bcb2-b573-4719-9412-cf6c3f4bbb15";
        final String name            = "BusinessCapability";
        final String description     = "Describes a function, capability or skill set.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "type";
        final String attribute1Description     = "Type of business capability.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the business capability.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("BusinessCapabilityType", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getGovernanceResponsibilityEntity()
    {
        final String guid            = "89a76b24-deb8-45bf-9304-a578a610326f";
        final String name            = "GovernanceResponsibility";
        final String description     = "Describes a responsibility of a person, team or organization that supports the implementation of a governance driver.";
        final String descriptionGUID = null;

        final String superTypeName = "OrganizationalControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGovernanceProcedureEntity()
    {
        final String guid            = "69055d10-51dc-4c2b-b21f-d76fad3f8ef3";
        final String name            = "GovernanceProcedure";
        final String description     = "Describes set of tasks that a person, team or organization performs to support the implementation of a governance driver.";
        final String descriptionGUID = null;

        final String superTypeName = "OrganizationalControl";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getOrganizationCapabilityRelationship()
    {
        final String guid            = "47f0ad39-db77-41b0-b406-36b1598e0ba7";
        final String name            = "OrganizationalCapability";
        final String description     = "Describes the relationship between a team and the business capabilities it supports.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "BusinessCapability";
        final String               end1AttributeName            = "supportsBusinessCapabilities";
        final String               end1AttributeDescription     = "The business capabilities that this team supports.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Team";
        final String               end2AttributeName            = "supportingTeams";
        final String               end2AttributeDescription     = "The teams that support this business capability.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getResponsibilityStaffContactRelationship()
    {
        final String guid            = "49f2ecb5-6bf7-4324-9824-ac98d595c404";
        final String name            = "ResponsibilityStaffContact";
        final String description     = "Identifies a person, team or engine assigned to a governance responsibility.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceResponsibility";
        final String               end1AttributeName            = "contactFor";
        final String               end1AttributeDescription     = "The governance responsibilities that this team or person is assigned to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ActorProfile";
        final String               end2AttributeName            = "assignedStaff";
        final String               end2AttributeDescription     = "The people, teams and/or engines that are supporting this governance responsibility.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "context";
        final String attribute1Description     = "The context in which this person, team or engine is to be contacted.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getBusinessCapabilityResponsibilityRelationship()
    {
        final String guid            = "b5de932a-738c-4c69-b852-09fec2b9c678";
        final String name            = "BusinessCapabilityResponsibility";
        final String description     = "Identifies a business capability that supports a governance responsibility.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceResponsibility";
        final String               end1AttributeName            = "supportingResponsibilities";
        final String               end1AttributeDescription     = "The governance responsibilities that this business capability supports.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "BusinessCapability";
        final String               end2AttributeName            = "affectedBusinessCapabilities";
        final String               end2AttributeDescription     = "The business capabilities that implement or support this governance responsibility.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "rationale";
        final String attribute1Description     = "Documents reasons for assigning the responsibility to this business capability.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0442ProjectCharter()
    {
        this.archiveBuilder.addEntityDef(getProjectCharterEntity());

        this.archiveBuilder.addRelationshipDef(getProjectCharterLinkRelationship());
    }


    private EntityDef getProjectCharterEntity()
    {
        final String guid            = "f96b5a32-42c1-4a74-8f77-70a81cec783d";
        final String name            = "ProjectCharter";
        final String description     = "Describes the goals, scope and authority of a project.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "mission";
        final String attribute1Description     = "The high-level goal of the project.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "projectType";
        final String attribute2Description     = "Short description of type of the project.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "purposes";
        final String attribute3Description     = "List of purposes for having the project.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getArrayStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);


        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getProjectCharterLinkRelationship()
    {
        final String guid            = "f081808d-545a-41cb-a9aa-c4f074a16c78";
        final String name            = "ProjectCharterLink";
        final String description     = "Links a Project with its Charter.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Project";
        final String               end1AttributeName            = "projects";
        final String               end1AttributeDescription     = "The projects guided by this charter.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_LEAST_ONE_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ProjectCharter";
        final String               end2AttributeName            = "charter";
        final String               end2AttributeDescription     = "The charter guiding this project.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0445GovernanceRoles()
    {
        this.archiveBuilder.addRelationshipDef(getStaffAssignmentRelationship());
    }


    private RelationshipDef getStaffAssignmentRelationship()
    {
        final String guid            = "cb10c107-b7af-475d-aab0-d78b8297b982";
        final String name            = "StaffAssignment";
        final String description     = "Identifies a person, team or engine assigned to reform a specific role.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "responsibilities";
        final String               end1AttributeDescription     = "The responsibilities of this person.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "Person";
        final String               end2AttributeName            = "staff";
        final String               end2AttributeDescription     = "The people assigned to this element.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Documents reasons for implementing the rule using this implementation.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0447GovernanceProcesses()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0450GovernanceRollout()
    {
        this.archiveBuilder.addEntityDef(getGovernanceMetricEntity());

        this.archiveBuilder.addRelationshipDef(getGovernanceDefinitionMetricRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceResultsRelationship());

        this.archiveBuilder.addClassificationDef(getGovernanceMeasurementsDataSetClassification());
    }


    private EntityDef getGovernanceMetricEntity()
    {
        final String guid            = "9ada8e7b-823c-40f7-adf8-f164aabda77e";
        final String name            = "GovernanceMetric";
        final String description     = "A definition for how the effectiveness of the governance program is measured.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Consumable name suitable for user interfaces and reports.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "description";
        final String attribute2Description     = "Description of the governance metric.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "measurement";
        final String attribute3Description     = "Format or description of the measurements captured for this metric.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "target";
        final String attribute4Description     = "Definition of the measurement values that the goverance definitions are trying to achieve..";
        final String attribute4DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getGovernanceDefinitionMetricRelationship()
    {
        final String guid            = "e076fbb3-54f5-46b8-8f1e-a7cb7e792673";
        final String name            = "GovernanceDefinitionMetric";
        final String description     = "Link between a governance definition and a governance metric used to measure this definition.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceMetric";
        final String               end1AttributeName            = "metrics";
        final String               end1AttributeDescription     = "The metrics that measure the landscape against this governance definition.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GovernanceDefinition";
        final String               end2AttributeName            = "measuredDefinitions";
        final String               end2AttributeDescription     = "The governance definitions that are measured by this metric.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "rationale";
        final String attribute1Description     = "Documents reasons for using the metric to measure the governance definition.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getGovernanceResultsRelationship()
    {
        final String guid            = "89c3c695-9e8d-4660-9f44-ed971fd55f88";
        final String name            = "GovernanceResults";
        final String description     = "Link between a governance metric and a data set used to gather measurements from the landscape.";
        final String descriptionGUID = null;


        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GovernanceMetric";
        final String               end1AttributeName            = "metrics";
        final String               end1AttributeDescription     = "The governance metrics that are captured in this data set.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "DataSet";
        final String               end2AttributeName            = "measurements";
        final String               end2AttributeDescription     = "The data set that captures the measurements for this governance metric.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "query";
        final String attribute1Description     = "Defines how the data items from the data set are converted in measurements for the metric.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private ClassificationDef getGovernanceMeasurementsDataSetClassification()
    {
        final String guid            = "789f2e89-accd-4489-8eca-dc43b432c022";
        final String name            = "GovernanceMeasurementsResultsDataSet";
        final String description     = "A data file containing measurements for a governance metric.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataSet";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "description";
        final String attribute1Description     = "Description of the use of the data set for governance metrics.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0452GovernanceDaemons()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0455ExceptionManagement()
    {
        this.archiveBuilder.addClassificationDef(getExceptionLogFileClassification());
        this.archiveBuilder.addClassificationDef(getAuditLogFileClassification());
        this.archiveBuilder.addClassificationDef(getExceptionBacklogClassification());
        this.archiveBuilder.addClassificationDef(getAuditLogClassification());
        this.archiveBuilder.addClassificationDef(getMeteringLogClassification());
        this.archiveBuilder.addClassificationDef(getStewardshipServerClassification());
        this.archiveBuilder.addClassificationDef(getGovernanceDaemonClassification());
    }


    private ClassificationDef getExceptionLogFileClassification()
    {
        final String guid            = "4756a6da-e0c2-4e81-b9ab-99df2f735eec";
        final String name            = "ExceptionLogFile";
        final String description     = "A data file containing exceptions.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataFile";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getAuditLogFileClassification()
    {
        final String guid            = "109d6d13-a3cf-4687-a0c1-c3802dc6b3a2";
        final String name            = "AuditLogFile";
        final String description     = "A data file containing audit log records.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataFile";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getExceptionBacklogClassification()
    {
        final String guid            = "b3eceea3-aa02-4d84-8f11-da4953e64b5f";
        final String name            = "ExceptionBacklog";
        final String description     = "A data set containing exceptions that need to be resolved";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataSet";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Notes on usage, purpose and type of exception backlog.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "steward";
        final String attribute2Description     = "Unique identifier of the person or team responsible for this exception backlog.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "process";
        final String attribute3Description     = "Unique identifier of the automated process that processes this exception backlog.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "source";
        final String attribute4Description     = "Source of the exception backlog.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getAuditLogClassification()
    {
        final String guid            = "449be034-6cc8-4f1b-859f-a8b9ff8ee7a1";
        final String name            = "AuditLog";
        final String description     = "A data set of related audit log records.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataSet";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Notes on usage, purpose and type of exception backlog.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "process";
        final String attribute2Description     = "Unique identifier of the automated process that processes this exception backlog.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "source";
        final String attribute3Description     = "Source of the exception backlog.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getMeteringLogClassification()
    {
        final String guid            = "161b37c9-1d51-433b-94ce-5a760a198236";
        final String name            = "MeteringLog";
        final String description     = "A data set containing records that can be used to identify usage of resources.";
        final String descriptionGUID = null;

        final String linkedToEntity = "DataSet";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "notes";
        final String attribute1Description     = "Notes on usage, purpose and type of exception backlog.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "process";
        final String attribute2Description     = "Unique identifier of the automated process that processes this exception backlog.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "source";
        final String attribute3Description     = "Source of the exception backlog.";
        final String attribute3DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getStewardshipServerClassification()
    {
        final String guid            = "eaaeaa31-6f8b-4ed5-88fe-422ed3733158";
        final String name            = "StewardshipServer";
        final String description     = "A server dedicated to managing stewardship activity relating to governance of data.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getGovernanceDaemonClassification()
    {
        final String guid            = "7815f222-529d-4902-8f0b-e37cbc779885";
        final String name            = "GovernanceDaemon";
        final String description     = "A server dedicated to managing activity relating to governance of data.";
        final String descriptionGUID = null;

        final String linkedToEntity = "SoftwareServer";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0460GovernanceActions()
    {
        this.archiveBuilder.addClassificationDef(getControlPointClassification());
        this.archiveBuilder.addClassificationDef(getVerificationPointClassification());
        this.archiveBuilder.addClassificationDef(getEnforcementPointClassification());
    }


    private ClassificationDef getControlPointClassification()
    {
        final String guid            = "acf8b73e-3545-435d-ba16-fbfae060dd28";
        final String name            = "ControlPoint";
        final String description     = "A task in a process where a person must make a decision on the right action to take.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getVerificationPointClassification()
    {
        final String guid            = "12d78c95-3879-466d-883f-b71f6477a741";
        final String name            = "VerificationPoint";
        final String description     = "A governance rule that tests if a required condition is true or raises an exception if not.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getEnforcementPointClassification()
    {
        final String guid            = "f4ce104e-7430-4c30-863d-60f6af6394d9";
        final String name            = "EnforcementPoint";
        final String description     = "A governance rule that ensures a required condition is true.";
        final String descriptionGUID = null;

        final String linkedToEntity = "Referenceable";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0480RightsManagement()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0481Licenses()
    {
        this.archiveBuilder.addEntityDef(getLicenseTypeEntity());

        this.archiveBuilder.addRelationshipDef(getLicenseRelationship());
    }


    private EntityDef getLicenseTypeEntity()
    {
        final String guid            = "046a049d-5f80-4e5b-b0ae-f3cf6009b513";
        final String name            = "LicenseType";
        final String description     = "A type of license that sets out specific terms and conditions for the use of an asset.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDefinition";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "details";
        final String attribute1Description     = "Description of the rights, terms and conditions associated with the licence.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getLicenseRelationship()
    {
        final String guid            = "35e53b7f-2312-4d66-ae90-2d4cb47901ee";
        final String name            = "License";
        final String description     = "Link between an asset and its license.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "licensed";
        final String               end1AttributeDescription     = "Items licensed by this type of license.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "LicenseType";
        final String               end2AttributeName            = "licenses";
        final String               end2AttributeDescription     = "The types of licenses that apply.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "licenseGUID";
        final String attribute1Description     = "Unique identifier of the actual license.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "start";
        final String attribute2Description     = "Start date for the license.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "end";
        final String attribute3Description     = "End date for the license.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "conditions";
        final String attribute4Description     = "Any special conditions or endorsements over the basic license type.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "licensedBy";
        final String attribute5Description     = "Person or organization that owns the intellectual property.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "custodian";
        final String attribute6Description     = "The person, engine or organization tht will ensure the license is honored.";
        final String attribute6DescriptionGUID = null;
        final String attribute7Name            = "licensee";
        final String attribute7Description     = "The person or organization that holds the license.";
        final String attribute7DescriptionGUID = null;
        final String attribute8Name            = "notes";
        final String attribute8Description     = "Additional notes about the license.";
        final String attribute8DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute7Name, attribute7Description, attribute7DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute8Name, attribute8Description, attribute8DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0482Certifications()
    {
        this.archiveBuilder.addEntityDef(getCertificationTypeEntity());

        this.archiveBuilder.addRelationshipDef(getCertificationRelationship());
        this.archiveBuilder.addRelationshipDef(getRegulationCertificationTypeRelationship());
    }


    private EntityDef getCertificationTypeEntity()
    {
        final String guid            = "97f9ffc9-e2f7-4557-ac12-925257345eea";
        final String name            = "CertificationType";
        final String description     = "A specific type of certification required by a regulation.";
        final String descriptionGUID = null;

        final String superTypeName = "GovernanceDefinition";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "details";
        final String attribute1Description     = "Description of the requirements associated with the certification.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getCertificationRelationship()
    {
        final String guid            = "390559eb-6a0c-4dd7-bc95-b9074caffa7f";
        final String name            = "Certification";
        final String description     = "An awarded certification of a specific type.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Referenceable";
        final String               end1AttributeName            = "certifies";
        final String               end1AttributeDescription     = "Items certified by this type of certification.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "CertificationType";
        final String               end2AttributeName            = "certifications";
        final String               end2AttributeDescription     = "The types of certifications that apply.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "certificateGUID";
        final String attribute1Description     = "Unique identifier of the actual certificate.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "start";
        final String attribute2Description     = "Start date for the certification.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "end";
        final String attribute3Description     = "End date for the certification.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "conditions";
        final String attribute4Description     = "Any special conditions or endorsements over the basic certification type.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "certifiedBy";
        final String attribute5Description     = "Person or organization awarded the certification.";
        final String attribute5DescriptionGUID = null;
        final String attribute6Name            = "custodian";
        final String attribute6Description     = "The person, engine or organization tht will ensure the certification is honored.";
        final String attribute6DescriptionGUID = null;
        final String attribute7Name            = "recipient";
        final String attribute7Description     = "The person or organization that received the certification.";
        final String attribute7DescriptionGUID = null;
        final String attribute8Name            = "notes";
        final String attribute8Description     = "Additional notes about the certification.";
        final String attribute8DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getDateTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute6Name, attribute6Description, attribute6DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute7Name, attribute7Description, attribute7DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute8Name, attribute8Description, attribute8DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    private RelationshipDef getRegulationCertificationTypeRelationship()
    {
        final String guid            = "be12ff15-0721-4a7e-8c98-334eaa884bdf";
        final String name            = "RegulationCertificationType";
        final String description     = "Identifies a certification required by a regulation.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Regulation";
        final String               end1AttributeName            = "relatedRegulations";
        final String               end1AttributeDescription     = "Regulations that require this type of certification.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "CertificationType";
        final String               end2AttributeName            = "requiredCertifications";
        final String               end2AttributeDescription     = "The certifications required by this regulation.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * ========================================
     * AREA 5 - standards
     */


    /**
     * Area 5 covers information architecture models and schemas.
     */
    private void addArea5Types()
    {
        this.add0501SchemaElements();
        this.add0503AssetSchemas();
        this.add0504ImplementationSnippets();
        this.add0505SchemaAttributes();
        this.add0510SchemaLinkElements();
        this.add0511SchemaMapElements();
        this.add0512DerivedSchemaAttributes();
        this.add0530TabularSchemas();
        this.add0531DocumentSchemas();
        this.add0532ObjectSchemas();
        this.add0533GraphSchemas();
        this.add0534RelationalSchemas();
        this.add0535EventSchemas();
        this.add0536APISchemas();
        this.add0550LogicSpecificationModel();
        this.add0560MappingModel();
        this.add0565ModelElements();
        this.add0570MetaModel();
        this.add0575ProcessSchemas();
        this.add580SolutionBlueprints();
        this.add0581SolutionPortsAndWires();
        this.addReferenceData();
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0501 Schema Elements - the structure of data is described by a schema.  Schemas are nested structures
     * of schema elements.
     */
    private void add0501SchemaElements()
    {
        this.archiveBuilder.addEntityDef(getSchemaElementEntity());
        this.archiveBuilder.addEntityDef(getSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getPrimitiveSchemaTypeEntity());
    }


    private EntityDef getSchemaElementEntity()
    {
        final String guid            = "718d4244-8559-49ed-ad5a-10e5c305a656";
        final String name            = "SchemaElement";
        final String description     = "An element that is part of a schema definition.";
        final String descriptionGUID = null;
        final String superTypeName   = "Referenceable";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);

    }


    private EntityDef getSchemaTypeEntity()
    {
        final String guid            = "5bd4a3e7-d22d-4a3d-a115-066ee8e0754f";
        final String name            = "SchemaType";
        final String description     = "A specific type description.";
        final String descriptionGUID = null;
        final String superTypeName   = "SchemaElement";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "displayName";
        final String attribute1Description     = "Display name for the schema type.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "versionNumber";
        final String attribute2Description     = "Version of the schema type.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "author";
        final String attribute3Description     = "User name of the person or process that created the schema type.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "usage";
        final String attribute4Description     = "Guidance on how the schema should be used.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "encodingStandard";
        final String attribute5Description     = "Format of the schema.";
        final String attribute5DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getPrimitiveSchemaTypeEntity()
    {
        final String guid            = "f0f75fba-9136-4082-8352-0ad74f3c36ed";
        final String name            = "PrimitiveSchemaType";
        final String description     = "A specific primitive type.";
        final String descriptionGUID = null;
        final String superTypeName   = "SchemaType";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "dataType";
        final String attribute1Description     = "Type name for the data stored in this schema element.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "defaultValue";
        final String attribute2Description     = "Initial value for data stored in this schema element.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0503 Asset Schemas shows how assets are linked to schemas
     */
    private void add0503AssetSchemas()
    {
        this.archiveBuilder.addRelationshipDef(getAssetSchemaTypeRelationship());
    }


    private RelationshipDef getAssetSchemaTypeRelationship()
    {
        final String guid            = "815b004d-73c6-4728-9dd9-536f4fe803cd";
        final String name            = "AssetSchemaType";
        final String description     = "The structure of an asset.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.TWO_TO_ONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Asset";
        final String               end1AttributeName            = "describesAssets";
        final String               end1AttributeDescription     = "Asset that conforms to the schema type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "schema";
        final String               end2AttributeDescription     = "Structure of the content of this asset.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0504 Implementation snippets enable code fragments defining data structures to be linked with the
     * relevant schema to show how the schema should be implemented.
     */
    private void add0504ImplementationSnippets()
    {
        this.archiveBuilder.addEntityDef(getImplementationSnippetEntity());

        this.archiveBuilder.addRelationshipDef(getSchemaTypeImplementationRelationship());
    }


    private EntityDef getImplementationSnippetEntity()
    {
        final String guid            = "49990755-2faa-4a62-a1f3-9124b9c73df4";
        final String name            = "ImplementationSnippet";
        final String description     = "A concrete implementation example for a schema element.";
        final String descriptionGUID = null;

        final String superTypeName = "Referenceable";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "snippet";
        final String attribute1Description     = "Concrete implementation of the schema type.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "implementationLanguage";
        final String attribute2Description     = "Type of implementation.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "version";
        final String attribute3Description     = "Version number of the snippet.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "usage";
        final String attribute4Description     = "Guidance on how the snippet should be used.";
        final String attribute4DescriptionGUID = null;
        final String attribute5Name            = "curator";
        final String attribute5Description     = "User name of the person or process that is maintaining the snippet.";
        final String attribute5DescriptionGUID = null;


        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute5Name, attribute5Description, attribute5DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getSchemaTypeImplementationRelationship()
    {
        final String guid            = "6aab4ec6-f0c6-4c40-9f50-ac02a3483358";
        final String name            = "SchemaTypeImplementation";
        final String description     = "Link between a schema type and an implementation snippet.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SchemaType";
        final String               end1AttributeName            = "schemaTypes";
        final String               end1AttributeDescription     = "Asset that conforms to the schema type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "ImplementationSnippet";
        final String               end2AttributeName            = "implementationSnippets";
        final String               end2AttributeDescription     = "Concrete implementation of the schema type.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0505 Schema Attributes begins to fill out the structure of a schema.
     */
    private void add0505SchemaAttributes()
    {
        this.archiveBuilder.addEntityDef(getSchemaAttributeEntity());
        this.archiveBuilder.addEntityDef(getComplexSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getStructSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getBoundedSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getArraySchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getSetSchemaTypeEntity());

        this.archiveBuilder.addRelationshipDef(getAttributeForSchemaRelationship());
        this.archiveBuilder.addRelationshipDef(getSchemaAttributeTypeRelationship());
    }


    private EntityDef getSchemaAttributeEntity()
    {
        final String guid            = "1a5e159b-913a-43b1-95fe-04433b25fca9";
        final String name            = "SchemaAttribute";
        final String description     = "A schema element that nests another schema type in its parent.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaElement";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "name";
        final String attribute1Description     = "Name of the attribute.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "position";
        final String attribute2Description     = "Position in the schema, starting at zero.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name            = "cardinality";
        final String attribute3Description     = "Number of occurrences of this attribute allowed.";
        final String attribute3DescriptionGUID = null;
        final String attribute4Name            = "defaultValueOverride";
        final String attribute4Description     = "Initial value for the attribute (overriding the default value of its type.";
        final String attribute4DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getIntTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getStringTypeDefAttribute(attribute4Name, attribute4Description, attribute4DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getComplexSchemaTypeEntity()
    {
        final String guid            = "786a6199-0ce8-47bf-b006-9ace1c5510e4";
        final String name            = "ComplexSchemaType";
        final String description     = "A schema type that has a complex structure of nested attributes and types.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getStructSchemaTypeEntity()
    {
        final String guid            = "a13b409f-fd67-4506-8d94-14dfafd250a4";
        final String name            = "StructSchemaType";
        final String description     = "A schema type that has a list of attributes, typically of different types.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getBoundedSchemaTypeEntity()
    {
        final String guid            = "77133161-37a9-43f5-aaa3-fd6d7ff92fdb";
        final String name            = "BoundedSchemaType";
        final String description     = "A schema type that limits the number of values that can be stored.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "maximumElements";
        final String attribute1Description     = "Maximum number of values that can be stored.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getIntTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private EntityDef getArraySchemaTypeEntity()
    {
        final String guid            = "ba8d29d2-a8a4-41f3-b29f-91ad924dd944";
        final String name            = "ArraySchemaType";
        final String description     = "A schema type that has a list of values of the same type.";
        final String descriptionGUID = null;

        final String superTypeName = "BoundedSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getSetSchemaTypeEntity()
    {
        final String guid            = "b2605d2d-10cd-443c-b3e8-abf15fb051f0";
        final String name            = "SetSchemaType";
        final String description     = "A schema type that is an unordered group of values of the same type.";
        final String descriptionGUID = null;

        final String superTypeName = "BoundedSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getAttributeForSchemaRelationship()
    {
        final String guid            = "86b176a2-015c-44a6-8106-54d5d69ba661";
        final String name            = "AttributeForSchema";
        final String description     = "Link between a complex schema type and its attributes.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.ONE_TO_TWO;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "ComplexSchemaType";
        final String               end1AttributeName            = "parentSchemas";
        final String               end1AttributeDescription     = "Schema types using this attribute.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaAttribute";
        final String               end2AttributeName            = "attributes";
        final String               end2AttributeDescription     = "The attributes defining the internal structure of the schema type.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getSchemaAttributeTypeRelationship()
    {
        final String guid            = "2d955049-e59b-45dd-8e62-cde1add59f9e";
        final String name            = "SchemaAttributeType";
        final String description     = "The schema type for an attribute.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.ONE_TO_TWO;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SchemaAttribute";
        final String               end1AttributeName            = "usedInSchemas";
        final String               end1AttributeDescription     = "Occurrences of this schema type in other schemas.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "type";
        final String               end2AttributeDescription     = "The structure of this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0510 Schema Link Elements defines how a link between two schema elements in different schema element
     * hierarchies are linked.
     */
    private void add0510SchemaLinkElements()
    {
        this.archiveBuilder.addEntityDef(getSchemaLinkElementEntity());

        this.archiveBuilder.addRelationshipDef(getLinkedTypeRelationship());
        this.archiveBuilder.addRelationshipDef(getSchemaLinkToTypeRelationship());
    }


    private EntityDef getSchemaLinkElementEntity()
    {
        final String guid            = "67e08705-2d2a-4df6-9239-1818161a41e0";
        final String name            = "SchemaLinkElement";
        final String description     = "A link to a type in a different schema.";
        final String descriptionGUID = null;
        final String superTypeName   = "SchemaElement";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "linkName";
        final String attribute1Description     = "Name for the element that bridges between two schemas.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name            = "linkProperties";
        final String attribute2Description     = "Any options needed to describe the link.";
        final String attribute2DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = archiveHelper.getMapStringStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getLinkedTypeRelationship()
    {
        final String guid            = "292125f7-5660-4533-a48a-478c5611922e";
        final String name            = "LinkedType";
        final String description     = "Link between a link element and its type.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SchemaLinkElement";
        final String               end1AttributeName            = "linkedBy";
        final String               end1AttributeDescription     = "External links to this type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "linkedTypes";
        final String               end2AttributeDescription     = "Types for this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getSchemaLinkToTypeRelationship()
    {
        final String guid            = "db9583c5-4690-41e5-a580-b4e30a0242d3";
        final String name            = "SchemaLinkToType";
        final String description     = "Link between a schema attribute and a schema link.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "SchemaAttribute";
        final String               end1AttributeName            = "usedIn";
        final String               end1AttributeDescription     = "Attributes of this type.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaLinkElement";
        final String               end2AttributeName            = "externalType";
        final String               end2AttributeDescription     = "External type for this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0511 Schema Map Elements defines how a map element is recorded in a schema.
     */
    private void add0511SchemaMapElements()
    {
        this.archiveBuilder.addEntityDef(getMapSchemaTypeEntity());

        this.archiveBuilder.addRelationshipDef(getMapFromElementTypeRelationship());
        this.archiveBuilder.addRelationshipDef(getMapToElementTypeRelationship());
    }


    private EntityDef getMapSchemaTypeEntity()
    {
        final String guid            = "bd4c85d0-d471-4cd2-a193-33b0387a19fd";
        final String name            = "MapSchemaType";
        final String description     = "A schema type for a map between a key and value.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getMapFromElementTypeRelationship()
    {
        final String guid            = "6189d444-2da4-4cd7-9332-e48a1c340b44";
        final String name            = "MapFromElementType";
        final String description     = "Defines the type of the key for a map schema type.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "MapSchemaType";
        final String               end1AttributeName            = "parentMapFrom";
        final String               end1AttributeDescription     = "Used in map.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "mapFromElement";
        final String               end2AttributeDescription     = "Key for this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getMapToElementTypeRelationship()
    {
        final String guid            = "8b9856b3-451e-45fc-afc7-fddefd81a73a";
        final String name            = "MapToElementType";
        final String description     = "Defines the type of value for a map schema type.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.ASSOCIATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "MapSchemaType";
        final String               end1AttributeName            = "parentMapTo";
        final String               end1AttributeDescription     = "Used in map.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "mapToElement";
        final String               end2AttributeDescription     = "Value for this map.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ONE_ONLY;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0512 Derived Schema Attributes defines how one attribute can be derived from another.
     */
    private void add0512DerivedSchemaAttributes()
    {
        this.archiveBuilder.addEntityDef(getDerivedSchemaAttributeEntity());

        this.archiveBuilder.addRelationshipDef(getSchemaQueryImplementationRelationship());
    }


    private EntityDef getDerivedSchemaAttributeEntity()
    {
        final String guid            = "cf21abfe-655a-47ba-b9b6-f73394745c80";
        final String name            = "DerivedSchemaAttribute";
        final String description     = "An attribute that is made up of values from another attribute.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        EntityDef entityDef = archiveHelper.getDefaultEntityDef(guid,
                                                                name,
                                                                this.archiveBuilder.getEntityDef(superTypeName),
                                                                description,
                                                                descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "formula";
        final String attribute1Description     = "Transformation used to create the derived data.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        entityDef.setPropertiesDefinition(properties);

        return entityDef;
    }


    private RelationshipDef getSchemaQueryImplementationRelationship()
    {
        final String guid            = "e5d7025d-8b4f-43c7-bcae-1047d650b94a";
        final String name            = "SchemaQueryImplementation";
        final String description     = "Details of how a derived schema attribute is calculated.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "DerivedSchemaAttribute";
        final String               end1AttributeName            = "usedBy";
        final String               end1AttributeDescription     = "Use of an attribute to derive another attribute.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaAttribute";
        final String               end2AttributeName            = "queryTarget";
        final String               end2AttributeDescription     = "Used to derive this attribute.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "query";
        final String attribute1Description     = "Details of how the attribute is retrieved.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        relationshipDef.setPropertiesDefinition(properties);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0530 describes table oriented schemas such as spreadsheets
     */
    private void add0530TabularSchemas()
    {
        this.archiveBuilder.addEntityDef(getTabularSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getTabularColumnTypeEntity());
        this.archiveBuilder.addEntityDef(getTabularColumnEntity());
    }


    private EntityDef getTabularSchemaTypeEntity()
    {
        final String guid            = "248975ec-8019-4b8a-9caf-084c8b724233";
        final String name            = "TabularSchemaType";
        final String description     = "A schema type for a table oriented data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getTabularColumnTypeEntity()
    {
        final String guid            = "a7392281-348d-48a4-bad7-f9742d7696fe";
        final String name            = "TabularColumnType";
        final String description     = "A schema type for a column oriented data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "PrimitiveSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getTabularColumnEntity()
    {
        final String guid            = "d81a0425-4e9b-4f31-bc1c-e18c3566da10";
        final String name            = "TabularColumn";
        final String description     = "A column attribute for a table oriented data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0531 Document Schema provide specialized entities for describing document oriented schemas such as JSON
     */
    private void add0531DocumentSchemas()
    {
        this.archiveBuilder.addEntityDef(getDocumentSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getDocumentSchemaAttributeEntity());
        this.archiveBuilder.addEntityDef(getSimpleDocumentTypeEntity());
        this.archiveBuilder.addEntityDef(getStructDocumentTypeEntity());
        this.archiveBuilder.addEntityDef(getArrayDocumentTypeEntity());
        this.archiveBuilder.addEntityDef(getSetDocumentTypeEntity());
        this.archiveBuilder.addEntityDef(getMapDocumentTypeEntity());
    }


    private EntityDef getDocumentSchemaTypeEntity()
    {
        final String guid            = "33da99cd-8d04-490c-9457-c58908da7794";
        final String name            = "DocumentSchemaType";
        final String description     = "A schema type for a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getDocumentSchemaAttributeEntity()
    {
        final String guid            = "b5cefb7e-b198-485f-a1d7-8e661012499b";
        final String name            = "DocumentSchemaAttribute";
        final String description     = "A schema attribute for a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getSimpleDocumentTypeEntity()
    {
        final String guid            = "42cfccbf-cc68-4980-8c31-0faf1ee002d3";
        final String name            = "SimpleDocumentType";
        final String description     = "A primitive attribute for a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "PrimitiveSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getStructDocumentTypeEntity()
    {
        final String guid            = "f6245c25-8f73-45eb-8fb5-fa17a5f27649";
        final String name            = "StructDocumentType";
        final String description     = "A structure within a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "StructSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getArrayDocumentTypeEntity()
    {
        final String guid            = "ddd29c67-db9a-45ff-92aa-6d17a12a8ee2";
        final String name            = "ArrayDocumentType";
        final String description     = "An array in a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "ArraySchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getSetDocumentTypeEntity()
    {
        final String guid            = "67228a7a-9d8d-4fa7-b217-17474f1f4ac6";
        final String name            = "SetDocumentType";
        final String description     = "A set in a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "SetSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getMapDocumentTypeEntity()
    {
        final String guid            = "b0f09598-ceb6-415b-befc-563ecadd5727";
        final String name            = "MapDocumentType";
        final String description     = "A map in a hierarchical data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "MapSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0532 Object Schemas provides the specialized entity for an object schema.
     */
    private void add0532ObjectSchemas()
    {
        this.archiveBuilder.addEntityDef(getObjectSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getObjectAttributeEntity());
    }


    private EntityDef getObjectSchemaTypeEntity()
    {
        final String guid            = "6920fda1-7c07-47c7-84f1-9fb044ae153e";
        final String name            = "ObjectSchemaType";
        final String description     = "A schema attribute for an object.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getObjectAttributeEntity()
    {
        final String guid            = "ccb408c0-582e-4a3a-a926-7082d53bb669";
        final String name            = "ObjectAttribute";
        final String description     = "An attribute in an object schema type.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0533GraphSchemas()
    {
        this.archiveBuilder.addEntityDef(getGraphSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getGraphVertexEntity());
        this.archiveBuilder.addEntityDef(getGraphEdgeEntity());

        this.archiveBuilder.addRelationshipDef(getGraphEdgeLinkRelationship());

    }


    private EntityDef getGraphSchemaTypeEntity()
    {
        final String guid            = "983c5e72-801b-4e42-bc51-f109527f2317";
        final String name            = "GraphSchemaType";
        final String description     = "A schema type for a graph data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGraphVertexEntity()
    {
        final String guid            = "1252ce12-540c-4724-ad70-f70940956de0";
        final String name            = "GraphVertex";
        final String description     = "A schema attribute for a graph data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getGraphEdgeEntity()
    {
        final String guid            = "d4104eb3-4f2d-4d83-aca7-e58dd8d5e0b1";
        final String name            = "GraphEdge";
        final String description     = "A schema attribute for a graph data structure.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getGraphEdgeLinkRelationship()
    {
        final String guid            = "503b4221-71c8-4ba9-8f3d-6a035b27971c";
        final String name            = "GraphEdgeLink";
        final String description     = "A relationship between a graph edge and a vertex.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "GraphEdge";
        final String               end1AttributeName            = "edges";
        final String               end1AttributeDescription     = "Edges for this vertex.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "GraphVertex";
        final String               end2AttributeName            = "vertices";
        final String               end2AttributeDescription     = "Vertices for this edge.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0534 Relational Schemas extend the tabular schemas to describe a relational database.
     */
    private void add0534RelationalSchemas()
    {
        this.archiveBuilder.addEntityDef(getRelationalDBSchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getRelationalTableTypeEntity());
        this.archiveBuilder.addEntityDef(getRelationalTableEntity());
        this.archiveBuilder.addEntityDef(getRelationalColumnEntity());
        this.archiveBuilder.addEntityDef(getRelationalColumnTypeEntity());
        this.archiveBuilder.addEntityDef(getDerivedRelationalColumnEntity());

        this.archiveBuilder.addClassificationDef(getPrimaryKeyClassification());
        this.archiveBuilder.addClassificationDef(getForeignKeyClassification());
        this.archiveBuilder.addClassificationDef(getRelationalViewClassification());

    }


    private EntityDef getRelationalDBSchemaTypeEntity()
    {
        final String guid            = "f20f5f45-1afb-41c1-9a09-34d8812626a4";
        final String name            = "RelationalDBSchemaType";
        final String description     = "A schema type for a relational database.";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getRelationalTableTypeEntity()
    {
        final String guid            = "1321bcc0-dc6a-48ed-9ca6-0c6f934b0b98";
        final String name            = "RelationalTableType";
        final String description     = "A table type for a relational database.";
        final String descriptionGUID = null;

        final String superTypeName = "TabularSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getRelationalTableEntity()
    {
        final String guid            = "ce7e72b8-396a-4013-8688-f9d973067425";
        final String name            = "RelationalTable";
        final String description     = "A table within a relational database schema type.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getRelationalColumnEntity()
    {
        final String guid            = "aa8d5470-6dbc-4648-9e2f-045e5df9d2f9";
        final String name            = "RelationalColumn";
        final String description     = "A column within a relational table.";
        final String descriptionGUID = null;

        final String superTypeName = "TabularColumn";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getRelationalColumnTypeEntity()
    {
        final String guid            = "f0438d80-6eb9-4fac-bcc1-5efee5babcfc";
        final String name            = "RelationalColumnType";
        final String description     = "A type for a relational column.";
        final String descriptionGUID = null;

        final String superTypeName = "TabularColumnType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getDerivedRelationalColumnEntity()
    {
        final String guid            = "a9f7d15d-b797-450a-8d56-1ba55490c019";
        final String name            = "DerivedRelationalColumn";
        final String description     = "A relational column that is derived from other columns.";
        final String descriptionGUID = null;

        final String superTypeName = "DerivedSchemaAttribute";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private ClassificationDef getPrimaryKeyClassification()
    {
        final String guid            = "b239d832-50bd-471b-b17a-15a335fc7f40";
        final String name            = "PrimaryKey";
        final String description     = "A uniquely identifying relational column.";
        final String descriptionGUID = null;

        final String linkedToEntity = "RelationalColumn";

        ClassificationDef classificationDef = archiveHelper.getClassificationDef(guid,
                                                                                 name,
                                                                                 null,
                                                                                 description,
                                                                                 descriptionGUID,
                                                                                 this.archiveBuilder.getEntityDef(linkedToEntity),
                                                                                 false);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name            = "keyPattern";
        final String attribute1Description     = "Type of primary key.";
        final String attribute1DescriptionGUID = null;

        property = archiveHelper.getEnumTypeDefAttribute("KeyPattern", attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);

        classificationDef.setPropertiesDefinition(properties);

        return classificationDef;
    }


    private ClassificationDef getForeignKeyClassification()
    {
        final String guid            = "3cd4e0e7-fdbf-47a6-ae88-d4b3205e0c07";
        final String name            = "ForeignKey";
        final String description     = "The primary key for another column is stored in this relational column.";
        final String descriptionGUID = null;

        final String linkedToEntity = "RelationalColumn";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }


    private ClassificationDef getRelationalViewClassification()
    {
        final String guid            = "4814bec8-482d-463d-8376-160b0358e129";
        final String name            = "RelationalView";
        final String description     = "A view within a relational database schema type.";
        final String descriptionGUID = null;

        final String linkedToEntity = "RelationalTable";

        return archiveHelper.getClassificationDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID,
                                                  this.archiveBuilder.getEntityDef(linkedToEntity),
                                                  false);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0535 Event Schemas describes event/message structures
     */
    private void add0535EventSchemas()
    {
        this.archiveBuilder.addEntityDef(getEventSetEntity());
        this.archiveBuilder.addEntityDef(getEventTypeEntity());
    }


    private EntityDef getEventSetEntity()
    {
        final String guid            = "bead9aa4-214a-4596-8036-aa78395bbfb1";
        final String name            = "EventSet";
        final String description     = "A collection of related event types.";
        final String descriptionGUID = null;

        final String superTypeName = "Collection";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getEventTypeEntity()
    {
        final String guid            = "8bc88aba-d7e4-4334-957f-cfe8e8eadc32";
        final String name            = "EventType";
        final String description     = "A description of an event (message)";
        final String descriptionGUID = null;

        final String superTypeName = "ComplexSchemaType";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0536 API schemas define the structure of an API
     */
    private void add0536APISchemas()
    {
        this.archiveBuilder.addEntityDef(getAPISchemaTypeEntity());
        this.archiveBuilder.addEntityDef(getAPIOperationSchemaEntity());

        this.archiveBuilder.addRelationshipDef(getAPIOperationsRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIHeaderRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIRequestRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIResponseRelationship());
    }


    private EntityDef getAPISchemaTypeEntity()
    {
        final String guid            = "b46cddb3-9864-4c5d-8a49-266b3fc95cb8";
        final String name            = "APISchemaType";
        final String description     = "Description of an API.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaElement";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private EntityDef getAPIOperationSchemaEntity()
    {
        final String guid            = "f1c0af19-2729-4fac-996e-a7badff3c21c";
        final String name            = "APIOperation";
        final String description     = "Description of an API operation.";
        final String descriptionGUID = null;

        final String superTypeName = "SchemaElement";

        return archiveHelper.getDefaultEntityDef(guid,
                                                 name,
                                                 this.archiveBuilder.getEntityDef(superTypeName),
                                                 description,
                                                 descriptionGUID);
    }


    private RelationshipDef getAPIOperationsRelationship()
    {
        final String guid            = "03737169-ceb5-45f0-84f0-21c5929945af";
        final String name            = "APIOperations";
        final String description     = "Link between an API and its operations.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "APISchemaType";
        final String               end1AttributeName            = "usedIn";
        final String               end1AttributeDescription     = "API that this operation belongs to.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.AT_LEAST_ONE_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "APIOperation";
        final String               end2AttributeName            = "contains";
        final String               end2AttributeDescription     = "Operations for this API type.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_LEAST_ONE_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAPIHeaderRelationship()
    {
        final String guid            = "e8fb46d1-5f75-481b-aa66-f43ad44e2cc6";
        final String name            = "APIHeader";
        final String description     = "Link between an API operation and its header.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "APIOperation";
        final String               end1AttributeName            = "usedAsAPIHeader";
        final String               end1AttributeDescription     = "API operations using this structure as the header.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "apiHeader";
        final String               end2AttributeDescription     = "Header structure for this API operation.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAPIRequestRelationship()
    {
        final String guid            = "4ab3b466-31bd-48ea-8aa2-75623476f2e2";
        final String name            = "APIRequest";
        final String description     = "Link between an API operation and its request structure.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "APIOperation";
        final String               end1AttributeName            = "usedAsAPIRequest";
        final String               end1AttributeDescription     = "API operations using this structure as the request body.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "apiRequest";
        final String               end2AttributeDescription     = "Request structure for this API operation.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    private RelationshipDef getAPIResponseRelationship()
    {
        final String guid            = "e8001de2-1bb1-442b-a66f-9addc3641eae";
        final String name            = "APIResponse";
        final String description     = "Link between and API operation and its response structure.";
        final String descriptionGUID = null;

        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef relationshipDef = archiveHelper.getBasicRelationshipDef(guid,
                                                                                name,
                                                                                null,
                                                                                description,
                                                                                descriptionGUID,
                                                                                relationshipCategory,
                                                                                classificationPropagationRule);

        RelationshipEndDef relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "APIOperation";
        final String               end1AttributeName            = "usedAsAPIResponse";
        final String               end1AttributeDescription     = "API operations using this structure as the response.";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
                                                                 end1AttributeName,
                                                                 end1AttributeDescription,
                                                                 end1AttributeDescriptionGUID,
                                                                 end1Cardinality);
        relationshipDef.setEndDef1(relationshipEndDef);


        /*
         * Set up end 2.
         */
        final String               end2EntityType               = "SchemaType";
        final String               end2AttributeName            = "apiResponse";
        final String               end2AttributeDescription     = "Response structure for this API operation.";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.AT_MOST_ONE;

        relationshipEndDef = archiveHelper.getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
                                                                 end2AttributeName,
                                                                 end2AttributeDescription,
                                                                 end2AttributeDescriptionGUID,
                                                                 end2Cardinality);
        relationshipDef.setEndDef2(relationshipEndDef);

        return relationshipDef;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0550LogicSpecificationModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0560MappingModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0565ModelElements()
    {
        this.archiveBuilder.addEntityDef(getModelElementEntity());

        this.archiveBuilder.addRelationshipDef(getModelImplementationRelationship());
    }


    private EntityDef getModelElementEntity()
    {
        final String guid = "492e343f-2516-43b8-94b0-5bae0760dda6";

        // TODO
        return null;
    }


    private RelationshipDef getModelImplementationRelationship()
    {
        final String guid = "4ff6d91b-3836-4ba2-9ca9-87da91081faa";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0570MetaModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void add0575ProcessSchemas()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 580 Solution Blueprints enable the recording of solution component models
     */
    private void add580SolutionBlueprints()
    {
        this.archiveBuilder.addEntityDef(getSolutionBlueprintEntity());
        this.archiveBuilder.addEntityDef(getSolutionBlueprintTemplateEntity());
        this.archiveBuilder.addEntityDef(getNestedSolutionBlueprintEntity());

        this.archiveBuilder.addRelationshipDef(getSolutionTypeRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionBlueprintComponentRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionBlueprintHierarchyRelationship());
    }


    private EntityDef getSolutionBlueprintEntity()
    {
        final String guid = "4aa47799-5128-4eeb-bd72-e357b49f8bfe";

        // TODO
        return null;
    }


    private EntityDef getSolutionBlueprintTemplateEntity()
    {
        final String guid = "f671e1fc-b204-4ee6-a4e2-da1633ecf50e";

        // TODO
        return null;
    }


    private EntityDef getNestedSolutionBlueprintEntity()
    {
        final String guid = "b83f3d42-f3f7-4155-ae65-58fb44ea7644";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionTypeRelationship()
    {
        final String guid = "f1ae975f-f11a-467b-8c7a-b023081e4712";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionBlueprintComponentRelationship()
    {
        final String guid = "a43b4c9c-52c2-4819-b3cc-9d07d49a11f2";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionBlueprintHierarchyRelationship()
    {
        final String guid = "2a9e56c3-bcf6-41de-bbe9-1e63b81d3114";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0581 Solution Ports and Wires defines how communication ports are connected to the solution components.
     */
    private void add0581SolutionPortsAndWires()
    {
        this.archiveBuilder.addEntityDef(getPortEntity());

        this.archiveBuilder.addRelationshipDef(getSolutionLinkingWireRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionPortRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionPortDelegationRelationship());
    }


    private EntityDef getPortEntity()
    {
        final String guid = "62ef448c-d4c1-4c94-a565-5e5625f6a57b";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionLinkingWireRelationship()
    {
        final String guid = "892a3d1c-cfb8-431d-bd59-c4d38833bfb0";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionPortRelationship()
    {
        final String guid = "5652d03a-f6c9-411a-a3e4-f490d3856b64";

        // TODO
        return null;
    }


    private RelationshipDef getSolutionPortDelegationRelationship()
    {
        final String guid = "8335e6ed-fd86-4000-9bc5-5203062f28ba";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    private void addReferenceData()
    {
        /* placeholder */
    }


    /*
     * ========================================
     * AREA 6 - metadata discovery
     */

    /**
     * Area 6 are the types for the discovery server (and discovery pipelines
     * supporting the Open Discovery Framework (ODF)).
     */
    private void addArea6Types()
    {
        this.add0601MetadataDiscoveryServer();
        this.add0605DiscoveryAnalysisReports();
        this.add0610Annotations();
        this.add0615SchemaExtraction();
        this.add0620Profiling();
        this.add0625SemanticDiscovery();
        this.add0630RelationshipDiscovery();
        this.add0635ClassificationDiscovery();
        this.add0650Measurements();
        this.add0660RequestForAction();

    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0601 Metadata Discovery Server defines how a metadata discovery server is represented in the metadata repository.
     */
    private void add0601MetadataDiscoveryServer()
    {
        this.archiveBuilder.addEntityDef(getDiscoveryServiceEntity());

        this.archiveBuilder.addClassificationDef(getMetadataDiscoveryServerClassification());
    }

    private EntityDef getDiscoveryServiceEntity()
    {
        final String guid = "2f278dfc-4640-4714-b34b-303e84e4fc40";

        // TODO
        return null;
    }

    private ClassificationDef getMetadataDiscoveryServerClassification()
    {
        final String guid = "be650674-790b-487a-a619-0a9002488055";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0605 Discovery Analysis Reports defines hoe the annotations from a discovery service are grouped
     * together and connected to the Assets they refer to.
     */
    private void add0605DiscoveryAnalysisReports()
    {
        this.archiveBuilder.addEntityDef(getDiscoveryAnalysisReportEntity());

        this.archiveBuilder.addRelationshipDef(getDiscoveryServerReportRelationship());
        this.archiveBuilder.addRelationshipDef(getDiscoveryServiceReportRelationship());
    }


    private EntityDef getDiscoveryAnalysisReportEntity()
    {
        final String guid = "acc7cbc8-09c3-472b-87dd-f78459323dcb";

        // TODO
        return null;
    }

    private RelationshipDef getDiscoveryServerReportRelationship()
    {
        final String guid = "2c318c3a-5dc2-42cd-a933-0087d852f67f";

        // TODO
        return null;
    }

    private RelationshipDef getDiscoveryServiceReportRelationship()
    {
        final String guid = "1744d72b-903d-4273-9229-de20372a17e2";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0610 Annotations describes the basic structure of an annotation created by a discovery service.
     */
    private void add0610Annotations()
    {
        this.archiveBuilder.addEnumDef(getAnnotationStatusEnum());

        this.archiveBuilder.addEntityDef(getAnnotationEntity());
        this.archiveBuilder.addEntityDef(getAnnotationReviewEntity());

        this.archiveBuilder.addRelationshipDef(getDiscoveredAnnotationRelationship());
        this.archiveBuilder.addRelationshipDef(getAssetAnnotationAttachmentRelationship());
        this.archiveBuilder.addRelationshipDef(getAnnotationReviewLinkRelationship());
    }

    private EnumDef getAnnotationStatusEnum()
    {
        final String guid = "71187df6-ef66-4f88-bc03-cd3c7f925165";

        // TODO
        return null;
    }

    private EntityDef getAnnotationEntity()
    {
        final String guid = "6cea5b53-558c-48f1-8191-11d48db29fb4";

        // TODO
        return null;
    }

    private EntityDef getAnnotationReviewEntity()
    {
        final String guid = "b893d6fc-642a-454b-beaf-809ee4dd876a";

        // TODO
        return null;
    }

    private RelationshipDef getDiscoveredAnnotationRelationship()
    {
        final String guid = "51d386a3-3857-42e3-a3df-14a6cad08b93";

        // TODO
        return null;
    }

    private RelationshipDef getAssetAnnotationAttachmentRelationship()
    {
        final String guid = "6056806d-682e-405c-964b-ca6fdc94be1b";

        // TODO
        return null;
    }

    private RelationshipDef getAnnotationReviewLinkRelationship()
    {
        final String guid = "5d3c2fb7-fa04-4d77-83cb-fd9216a07769";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0615 Schema Extraction creates an annotation to describe a nested schema.
     */
    private void add0615SchemaExtraction()
    {
        this.archiveBuilder.addEntityDef(getSchemaAnnotationEntity());

        this.archiveBuilder.addRelationshipDef(getNestedSchemaStructureRelationship());
    }

    private EntityDef getSchemaAnnotationEntity()
    {
        final String guid = "3c5aa68b-d562-4b04-b189-c7b7f0bf2ced";

        // TODO
        return null;
    }

    private RelationshipDef getNestedSchemaStructureRelationship()
    {
        final String guid = "60f2d263-e24d-4f20-8c0d-b5e24648cd54";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0620 Profiling describes an annotation that can be attached to a column to describe the profile of its
     * values.
     */
    private void add0620Profiling()
    {
        this.archiveBuilder.addEntityDef(getProfilingAnnotationEntity());
    }

    private EntityDef getProfilingAnnotationEntity()
    {
        final String guid = "bff1f694-afd0-4829-ab11-50a9fbaf2f5f";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0625 Semantic Discovery describes an annotation for a candidate glossary term assignment for a data element.
     */
    private void add0625SemanticDiscovery()
    {
        this.archiveBuilder.addEntityDef(getSemanticAnnotationEntity());
    }

    private EntityDef getSemanticAnnotationEntity()
    {
        final String guid = "0b494819-28be-4604-b238-3af20963eea6";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0630 Relationship Discovery provides a base annotation for describing a relationship between two referenceables.
     */
    private void add0630RelationshipDiscovery()
    {
        this.archiveBuilder.addRelationshipDef(getRelationshipAnnotationRelationship());
    }

    private RelationshipDef getRelationshipAnnotationRelationship()
    {
        final String guid = "73510abd-49e6-4097-ba4b-23bd3ef15baa";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0635 Classification Discovery creates a base class for a classification annotation.
     */
    private void add0635ClassificationDiscovery()
    {
        this.archiveBuilder.addClassificationDef(getClassificationAnnotationClassification());
    }


    private ClassificationDef getClassificationAnnotationClassification()
    {
        final String guid = "23e8287f-5c7e-4e03-8bd3-471fc7fc029c";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0650 Measurements describe annotations that are measuring characteristics of an Asset.
     */
    private void add0650Measurements()
    {
        this.archiveBuilder.addEntityDef(getDataSetMeasurementAnnotationEntity());
        this.archiveBuilder.addEntityDef(getDataSetPhysicalStatusAnnotationEntity());
    }

    private EntityDef getDataSetMeasurementAnnotationEntity()
    {
        final String guid = "c85bea73-d7af-46d7-8a7e-cb745910b1df";

        // TODO
        return null;
    }

    private EntityDef getDataSetPhysicalStatusAnnotationEntity()
    {
        final String guid = "e9ba276e-6d9f-4999-a5a9-9ddaaabfae23";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0660 Request for Action creates an annotation for requesting a stewardship or governance action.
     */
    private void add0660RequestForAction()
    {
        this.archiveBuilder.addEntityDef(getRequestForActionAnnotationEntity());
    }

    private EntityDef getRequestForActionAnnotationEntity()
    {
        final String guid = "f45765a9-f3ae-4686-983f-602c348e020d";

        // TODO
        return null;
    }

    /*
     * ========================================
     * AREA 7 - lineage
     */

    /**
     * Add the types for lineage
     */
    private void addArea7Types()
    {
        /*
         * The types for area 7 are not yet defined, this method is a placeholder.
         */
    }
}
