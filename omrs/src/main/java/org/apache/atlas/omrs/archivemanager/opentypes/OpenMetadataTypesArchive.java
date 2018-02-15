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
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchive;
import org.apache.atlas.omrs.archivemanager.properties.OpenMetadataArchiveType;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceStatus;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * OpenMetadataTypesArchive builds an open metadata archive containing all of the standard open metadata types.
 * These types have hardcoded dates and guids so that however many times this archive is rebuilt, it will
 * produce the same content.
 * <p>
 *     Details of the open metadata types are documented on the wiki:
 *     <a href="https://cwiki.apache.org/confluence/display/ATLAS/Building+out+the+Open+Metadata+Typesystem">Building out the Open Metadata Typesystem</a>
 * </p>
 * <p>
 *     There are 8 areas, each covering a different topic area of metadata.  The module breaks down the process of creating
 *     the models into the areas and then the individual models to simplify the maintenance of this class
 * </p>
 */
public class OpenMetadataTypesArchive
{
    /*
     * This is the header information for the archive.
     */
    private static final  String                  archiveGUID        = "bce3b0a0-662a-4f87-b8dc-844078a11a6e";
    private static final  String                  archiveName        = "Open Metadata Types";
    private static final  String                  archiveDescription = "Standard types for open metadata repositories.";
    private static final  OpenMetadataArchiveType archiveType        = OpenMetadataArchiveType.CONTENT_PACK;
    private static final  String                  originatorName     = "Apache Atlas (OMRS)";
    private static final  Date                    creationDate       = new Date(1516313040008L);

    /*
     * Specific values for initializing TypeDefs
     */
    private static final  long                    versionNumber      = 1L;
    private static final  String                  versionName        = "1.0";



    private OMRSArchiveBuilder      archiveBuilder;


    /**
     * Default constructor sets up the archive builder.  This in turn sets up the header for the archive.
     */
    public OpenMetadataTypesArchive()
    {
        OMRSArchiveBuilder      newArchiveBuilder = new OMRSArchiveBuilder(archiveGUID,
                                                                           archiveName,
                                                                           archiveDescription,
                                                                           archiveType,
                                                                           originatorName,
                                                                           creationDate,
                                                                           null);
        this.archiveBuilder = newArchiveBuilder;
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
            OMRSErrorCode errorCode = OMRSErrorCode.ARCHIVE_UNAVAILABLE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

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
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BOOLEAN));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BYTE));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_CHAR));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_SHORT));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_INT));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_LONG));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_FLOAT));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DOUBLE));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGINTEGER));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGDECIMAL));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING));
        this.archiveBuilder.addPrimitiveDef(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DATE));
    }


    /**
     * Set up an individual primitive definition
     *
     * @param primitiveDefCategory - category of the primitive def defines the unique
     *                             information about this primitive type.
     * @return initialized PrimitiveDef object ready for the archive
     */
    private PrimitiveDef getPrimitiveDef(PrimitiveDefCategory primitiveDefCategory)
    {
        PrimitiveDef  primitiveDef = new PrimitiveDef(primitiveDefCategory);

        primitiveDef.setGUID(primitiveDefCategory.getGUID());
        primitiveDef.setName(primitiveDefCategory.getName());

        return primitiveDef;
    }


    /**
     * Create a CollectionDef.  A new CollectionDef is required for each combination of primitive types
     * used to initialize the collection.  Each CollectionDef has its own unique identifier (guid) and
     * its name is a combination of the collection type and the primitives use to initialize it.
     *
     * @param guid - unique identifier for the CollectionDef
     * @param name - unique name for the CollectionDef
     * @param collectionDefCategory - category of the collection.
     * @return Filled out CollectionDef
     */
    private CollectionDef getCollectionDef(String                guid,
                                           String                name,
                                           CollectionDefCategory collectionDefCategory)
    {
        CollectionDef   collectionDef = new CollectionDef(collectionDefCategory);

        collectionDef.setGUID(guid);
        collectionDef.setName(name);

        return collectionDef;
    }


    /**
     * Create an EnumDef that has no valid values defined.  These are added by the caller.
     *
     * @param guid - unique identifier for the CollectionDef
     * @param name - unique name for the CollectionDef
     * @param description - short default description of the enum type
     * @param descriptionGUID - guid of the glossary term describing this enum type
     * @return basic EnumDef without valid values
     */
    private EnumDef getEmptyEnumDef(String                guid,
                                    String                name,
                                    String                description,
                                    String                descriptionGUID)
    {
        EnumDef  enumDef = new EnumDef();

        enumDef.setGUID(guid);
        enumDef.setName(name);
        enumDef.setDescription(description);
        enumDef.setDescriptionGUID(descriptionGUID);
        enumDef.setDefaultValue(null);

        return enumDef;
    }


    /**
     * Create an EnumElementDef that carries one of the valid values for an Enum.
     *
     * @param ordinal - code number
     * @param value - name
     * @param description - short description
     * @param descriptionGUID - guid of the glossary term describing this enum element
     * @return Fully filled out EnumElementDef
     */
    private EnumElementDef  getEnumElementDef(int     ordinal,
                                              String  value,
                                              String  description,
                                              String  descriptionGUID)
    {
        EnumElementDef   enumElementDef = new EnumElementDef();

        enumElementDef.setOrdinal(ordinal);
        enumElementDef.setValue(value);
        enumElementDef.setDescription(description);
        enumElementDef.setDescriptionGUID(descriptionGUID);

        return enumElementDef;
    }


    /**
     * Sets up a default EntityDef.  Calling methods can override the default values.  This EntityDef
     * has no attribute defined.
     *
     * @param guid - unique identifier for the entity
     * @param name - name of the entity
     * @param superType - Super type for this entity (null for top-level)
     * @param description - short description of the entity
     * @param descriptionGUID - guid of the glossary term describing this entity type
     * @return Initialized EntityDef
     */
    EntityDef  getDefaultEntityDef(String                  guid,
                                   String                  name,
                                   TypeDefLink             superType,
                                   String                  description,
                                   String                  descriptionGUID)
    {
        EntityDef entityDef = new EntityDef();

        /*
         * Set up the parameters supplied by the caller.
         */
        entityDef.setGUID(guid);
        entityDef.setName(name);
        entityDef.setSuperType(superType);
        entityDef.setDescription(description);
        entityDef.setDescriptionGUID(descriptionGUID);

        /*
         * Set up the defaults
         */
        entityDef.setOrigin(archiveGUID);
        entityDef.setCreatedBy(originatorName);
        entityDef.setCreateTime(creationDate);
        entityDef.setVersion(versionNumber);
        entityDef.setVersionName(versionName);

        /*
         * Set default valid instance statuses
         */
        ArrayList<InstanceStatus>    validInstanceStatusList  = new ArrayList<>();
        validInstanceStatusList.add(InstanceStatus.ACTIVE);
        validInstanceStatusList.add(InstanceStatus.DELETED);
        entityDef.setValidInstanceStatusList(validInstanceStatusList);

        return entityDef;
    }


    /**
     * Return an attribute with the supplied name and description that is of type String.  It is set up to be optional,
     * indexable (useful for searches) but the value does not need to be unique.
     * These are the typical values used for most open metadata attribute.
     * They can be changed by the caller once the TypeDefAttribute is returned.
     *
     * @param attributeName - name of the attribute
     * @param attributeDescription - short description for the attribute
     * @param attributeDescriptionGUID - guid of the glossary term that describes this attribute.
     * @return Optional TypeDefAttribute of type string
     */
    TypeDefAttribute  getStringTypeDefAttribute(String      attributeName,
                                                String      attributeDescription,
                                                String      attributeDescriptionGUID)
    {
        TypeDefAttribute     attribute = new TypeDefAttribute();

        attribute.setAttributeName(attributeName);
        attribute.setAttributeDescription(attributeDescription);
        attribute.setAttributeDescriptionGUID(attributeDescriptionGUID);
        attribute.setAttributeType(getPrimitiveDef(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING));
        attribute.setAttributeCardinality(AttributeCardinality.AT_MOST_ONE);
        attribute.setValuesMinCount(0);
        attribute.setValuesMaxCount(1);
        attribute.setIndexable(true);
        attribute.setUnique(false);
        attribute.setDefaultValue(null);
        attribute.setExternalStandardMappings(null);

        return attribute;
    }


    /**
     * Returns a basic RelationshipDef without any attributes or ends set up.
     * The caller is responsible for adding the attributes and ends definition.
     *
     * @param guid - unique identifier for the relationship
     * @param name - name of the relationship
     * @param superType - Super type for this relationship (null for top-level)
     * @param description - short default description of the relationship
     * @param descriptionGUID - guid of the glossary term that describes this relationship
     * @param relationshipCategory - is this an association, aggregation or composition?
     * @param propagationRule - should classifications propagate over this relationship?
     * @return RelationshipDef with no ends defined.
     */
    private RelationshipDef getBasicRelationshipDef(String                        guid,
                                                    String                        name,
                                                    TypeDefLink                   superType,
                                                    String                        description,
                                                    String                        descriptionGUID,
                                                    RelationshipCategory          relationshipCategory,
                                                    ClassificationPropagationRule propagationRule)
    {
        RelationshipDef relationshipDef = new RelationshipDef();

        /*
         * Set up the parameters supplied by the caller.
         */
        relationshipDef.setGUID(guid);
        relationshipDef.setName(name);
        relationshipDef.setSuperType(superType);
        relationshipDef.setDescription(description);
        relationshipDef.setDescriptionGUID(descriptionGUID);

        /*
         * Set up the defaults
         */
        relationshipDef.setOrigin(archiveGUID);
        relationshipDef.setCreatedBy(originatorName);
        relationshipDef.setCreateTime(creationDate);
        relationshipDef.setVersion(versionNumber);
        relationshipDef.setVersionName(versionName);

        /*
         * Set default valid instance statuses
         */
        ArrayList<InstanceStatus>    validInstanceStatusList  = new ArrayList<>();
        validInstanceStatusList.add(InstanceStatus.ACTIVE);
        validInstanceStatusList.add(InstanceStatus.DELETED);
        relationshipDef.setValidInstanceStatusList(validInstanceStatusList);

        /*
         * Set up the category of relationship
         */
        relationshipDef.setRelationshipCategory(relationshipCategory);

        if ((relationshipCategory == RelationshipCategory.AGGREGATION) ||
            (relationshipCategory == RelationshipCategory.COMPOSITION))
        {
            /*
             * By convention, end 1 is the container end in the open metadata model.
             */
            relationshipDef.setRelationshipContainerEnd(RelationshipContainerEnd.END1);
        }
        else
        {
            relationshipDef.setRelationshipContainerEnd(RelationshipContainerEnd.NOT_APPLICABLE);
        }

        /*
         * Use the supplied propagation rule.
         */
        relationshipDef.setPropagationRule(propagationRule);

        return relationshipDef;
    }


    /**
     * Returns a RelationshipEndDef object that sets up details of an entity at one end of a relationship.
     *
     * @param entityType - details of the type of entity connected to this end.
     * @param attributeName - name of the attribute that the entity at the other end uses to refer to this entity.
     * @param attributeDescription - description of this attribute
     * @param attributeDescriptionGUID - unique identifier of the glossary term describing this attribute.
     * @param attributeCardinality - cardinality of this end of the relationship.
     * @return the definition of one end of a Relationship.
     */
    private RelationshipEndDef  getRelationshipEndDef(TypeDefLink          entityType,
                                                      String               attributeName,
                                                      String               attributeDescription,
                                                      String               attributeDescriptionGUID,
                                                      AttributeCardinality attributeCardinality)
    {
        RelationshipEndDef  relationshipEndDef = new RelationshipEndDef();

        relationshipEndDef.setEntityType(entityType);
        relationshipEndDef.setAttributeName(attributeName);
        relationshipEndDef.setAttributeDescription(attributeDescription);
        relationshipEndDef.setAttributeDescriptionGUID(attributeDescriptionGUID);
        relationshipEndDef.setAttributeCardinality(attributeCardinality);

        return relationshipEndDef;
    }


    /**
     * Returns a basic ClassificationDef without any attributes.   The caller is responsible for adding the
     * attribute definitions.
     *
     * @param guid - unique identifier for the classification
     * @param name - name of the classification
     * @param superType - Super type for this classification (null for top-level)
     * @param description - short description of the classification
     * @param descriptionGUID - unique identifier of the glossary term that describes this classification.
     * @param validEntityDefs - which entities can this classification be linked to.
     * @param propagatable - can the classification propagate over relationships?
     * @return ClassificationDef with no attributes defined.
     */
    private ClassificationDef getClassificationDef(String                        guid,
                                                   String                        name,
                                                   TypeDefLink                   superType,
                                                   String                        description,
                                                   String                        descriptionGUID,
                                                   ArrayList<TypeDefLink>        validEntityDefs,
                                                   boolean                       propagatable)
    {
        ClassificationDef classificationDef = new ClassificationDef();

        /*
         * Set up the parameters supplied by the caller.
         */
        classificationDef.setGUID(guid);
        classificationDef.setName(name);
        classificationDef.setSuperType(superType);
        classificationDef.setDescription(description);
        classificationDef.setDescriptionGUID(descriptionGUID);

        /*
         * Set up the defaults
         */
        classificationDef.setOrigin(archiveGUID);
        classificationDef.setCreatedBy(originatorName);
        classificationDef.setCreateTime(creationDate);
        classificationDef.setVersion(versionNumber);
        classificationDef.setVersionName(versionName);

        /*
         * Set default valid instance statuses
         */
        ArrayList<InstanceStatus>    validInstanceStatusList  = new ArrayList<>();
        validInstanceStatusList.add(InstanceStatus.ACTIVE);
        validInstanceStatusList.add(InstanceStatus.DELETED);
        classificationDef.setValidInstanceStatusList(validInstanceStatusList);

        /*
         * Set up the supplied validEntityTypes and propagatable flag.
         */
        classificationDef.setValidEntityDefs(validEntityDefs);
        classificationDef.setPropagatable(propagatable);

        return classificationDef;
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
        final String guid            = "005c7c14-ac84-4136-beed-959401b041f8";
        final String name            = "Referenceable";
        final String description     = "An open metadata entity that has a unique identifier.";
        final String descriptionGUID = null;

        EntityDef entityDef = getDefaultEntityDef(guid,
                                                  name,
                                                  null,
                                                  description,
                                                  descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attributeName = "qualifiedName";
        final String attributeDescription = "Unique identifier for the entity.";
        final String attributeDescriptionGUID = null;


        property = getStringTypeDefAttribute(attributeName, attributeDescription, attributeDescriptionGUID);
        property.setUnique(true);
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

        EntityDef entityDef = getDefaultEntityDef(guid,
                                                  name,
                                                  this.archiveBuilder.getEntityDef(superTypeName),
                                                  description,
                                                  descriptionGUID);

        /*
         * Build the attributes
         */
        ArrayList<TypeDefAttribute> properties = new ArrayList<>();
        TypeDefAttribute            property;

        final String attribute1Name = "name";
        final String attribute1Description = "Display name for the asset.";
        final String attribute1DescriptionGUID = null;
        final String attribute2Name = "description";
        final String attribute2Description = "Description of the asset.";
        final String attribute2DescriptionGUID = null;
        final String attribute3Name = "owner";
        final String attribute3Description = "User name of the person or process that owns the asset.";
        final String attribute3DescriptionGUID = null;


        property = getStringTypeDefAttribute(attribute1Name, attribute1Description, attribute1DescriptionGUID);
        properties.add(property);
        property = getStringTypeDefAttribute(attribute2Name, attribute2Description, attribute2DescriptionGUID);
        properties.add(property);
        property = getStringTypeDefAttribute(attribute3Name, attribute3Description, attribute3DescriptionGUID);
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
        final String guid = "c19746ac-b3ec-49ce-af4b-83348fc55e07";
        final String name = "Infrastructure";
        final String description = "Physical infrastructure or software platform.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        EntityDef entityDef = getDefaultEntityDef(guid,
                                                  name,
                                                  this.archiveBuilder.getEntityDef(superTypeName),
                                                  description,
                                                  descriptionGUID);

        return entityDef;
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
        final String guid = "d8f33bd7-afa9-4a11-a8c7-07dcec83c050";
        final String name = "Process";
        final String description = "Well-defined sequence of activities performed by people or software components.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        EntityDef entityDef = getDefaultEntityDef(guid,
                                                  name,
                                                  this.archiveBuilder.getEntityDef(superTypeName),
                                                  description,
                                                  descriptionGUID);

        return entityDef;
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
        final String guid = "1449911c-4f44-4c22-abc0-7540154feefb";
        final String name = "DataSet";
        final String description = "Collection of related data.";
        final String descriptionGUID = null;
        final String superTypeName   = "Asset";

        EntityDef entityDef = getDefaultEntityDef(guid,
                                                  name,
                                                  this.archiveBuilder.getEntityDef(superTypeName),
                                                  description,
                                                  descriptionGUID);

        return entityDef;
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
        final String                        guid                          = "9a6583c4-7419-4d5a-a6e5-26b0033fa349";
        final String                        name                          = "ProcessInput";
        final String                        description                   = "The DataSets passed into a Process.";
        final String                        descriptionGUID               = null;
        final RelationshipCategory          relationshipCategory          = RelationshipCategory.AGGREGATION;
        final ClassificationPropagationRule classificationPropagationRule = ClassificationPropagationRule.NONE;

        RelationshipDef   relationshipDef = getBasicRelationshipDef(guid,
                                                                    name,
                                                                    null,
                                                                    description,
                                                                    descriptionGUID,
                                                                    relationshipCategory,
                                                                    classificationPropagationRule);

        RelationshipEndDef     relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Process";
        final String               end1AttributeName            = "consumedByProcess";
        final String               end1AttributeDescription     = "Processes that consume this DataSet";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
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
        final String               end2AttributeDescription     = "DataSets consumed by this Process";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
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

        RelationshipDef   relationshipDef = getBasicRelationshipDef(guid,
                                                                    name,
                                                                    null,
                                                                    description,
                                                                    descriptionGUID,
                                                                    relationshipCategory,
                                                                    classificationPropagationRule);

        RelationshipEndDef     relationshipEndDef;

        /*
         * Set up end 1.
         */
        final String               end1EntityType               = "Process";
        final String               end1AttributeName            = "producedByProcess";
        final String               end1AttributeDescription     = "Processes that produce this DataSet";
        final String               end1AttributeDescriptionGUID = null;
        final AttributeCardinality end1Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = getRelationshipEndDef(this.archiveBuilder.getEntityDef(end1EntityType),
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
        final String               end2AttributeDescription     = "DataSets produced by this Process";
        final String               end2AttributeDescriptionGUID = null;
        final AttributeCardinality end2Cardinality              = AttributeCardinality.ANY_NUMBER_UNORDERED;

        relationshipEndDef = getRelationshipEndDef(this.archiveBuilder.getEntityDef(end2EntityType),
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
        this.archiveBuilder.addEnumDef(getMediaUsageEnum());

        this.archiveBuilder.addEntityDef(getExternalReferenceEntity());
        this.archiveBuilder.addEntityDef(getRelatedMediaEntity());

        this.archiveBuilder.addRelationshipDef(getExternalReferenceLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getMediaReferenceRelationship());
    }

    private EnumDef  getMediaUsageEnum()
    {
        final String guid = "c6861a72-7485-48c9-8040-876f6c342b61";
        final String name = "MediaUsage";
        final String description = "Defines how a related media reference should be used.";
        final String descriptionGUID = null;

        EnumDef enumDef = getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs    = new ArrayList();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Icon";
        final String element1Description     = "Provides a small image to represent the asset in tree views and graphs.";
        final String element1DescriptionGUID = null;

        elementDef = getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Thumbnail";
        final String element2Description     = "Provides a small image about the asset that can be used in lists.";
        final String element2DescriptionGUID = null;

        elementDef = getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Illustration";
        final String element3Description     = "Illustrates how the asset works or what it contains. It is complementary to the asset's description.";
        final String element3DescriptionGUID = null;

        elementDef = getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Usage Guidance";
        final String element4Description     = "Provides guidance to a person on how to use the asset.";
        final String element4DescriptionGUID = null;

        elementDef = getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 99;
        final String element5Value           = "Other";
        final String element5Description     = "Another usage.";
        final String element5DescriptionGUID = null;

        elementDef = getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        enumDef.setElementDefs(elementDefs);

        return enumDef;
    }

    private EntityDef  getExternalReferenceEntity()
    {
        final String guid = "af536f20-062b-48ef-9c31-1ddd05b04c56";
        final String name            = "ExternalReference";
        final String description     = "A link to more information.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private EntityDef  getRelatedMediaEntity()
    {
        final String guid = "747f8b86-fe7c-4c9b-ba75-979e093cc307";
        final String name            = "RelatedMedia";
        final String description     = "Images, video or sound media.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getExternalReferenceLinkRelationship()
    {
        final String guid = "7d818a67-ab45-481c-bc28-f6b1caf12f06";
        final String name            = "ExternalReferenceLink";
        final String description     = "Link to more information.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getMediaReferenceRelationship()
    {
        final String guid = "1353400f-b0ab-4ab9-ab09-3045dd8a7140";
        final String name            = "MediaReference";
        final String description     = "Link to related media such as images, videos and audio.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EnumDef  getKeyPatternEnum()
    {
        final String guid = "8904df8f-1aca-4de8-9abd-1ef2aadba300";
        final String name = "KeyPattern";
        final String description = "Defines the type of identifier used for an asset.";
        final String descriptionGUID = null;

        EnumDef enumDef = getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs    = new ArrayList();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Local Key";
        final String element1Description     = "Unique key allocated and used within the scope of a single system.";
        final String element1DescriptionGUID = null;

        elementDef = getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Recycled Key";
        final String element2Description     = "Key allocated and used within the scope of a single system that is periodically reused for different records.";
        final String element2DescriptionGUID = null;

        elementDef = getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element3Ordinal         = 2;
        final String element3Value           = "Natural Key";
        final String element3Description     = "Key derived from an attribute of the entity, such as email address, passport number.";
        final String element3DescriptionGUID = null;

        elementDef = getEnumElementDef(element3Ordinal, element3Value, element3Description, element3DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element4Ordinal         = 3;
        final String element4Value           = "Mirror Key";
        final String element4Description     = "Key value copied from another system.";
        final String element4DescriptionGUID = null;

        elementDef = getEnumElementDef(element4Ordinal, element4Value, element4Description, element4DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element5Ordinal         = 4;
        final String element5Value           = "Aggregate Key";
        final String element5Description     = "Key formed by combining keys from multiple systems.";
        final String element5DescriptionGUID = null;

        elementDef = getEnumElementDef(element5Ordinal, element5Value, element5Description, element5DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element6Ordinal         = 5;
        final String element6Value           = "Caller's Key";
        final String element6Description     = "Key from another system can bey used if system name provided.";
        final String element6DescriptionGUID = null;

        elementDef = getEnumElementDef(element6Ordinal, element6Value, element6Description, element6DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element7Ordinal         = 6;
        final String element7Value           = "Stable Key";
        final String element7Description     = "Key value will remain active even if records are merged.";
        final String element7DescriptionGUID = null;

        elementDef = getEnumElementDef(element7Ordinal, element7Value, element7Description, element7DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element8Ordinal         = 99;
        final String element8Value           = "Other";
        final String element8Description     = "Another key pattern.";
        final String element8DescriptionGUID = null;

        elementDef = getEnumElementDef(element8Ordinal, element8Value, element8Description, element8DescriptionGUID);
        elementDefs.add(elementDef);

        return enumDef;
    }

    private EntityDef  getExternalIdEntity()
    {
        final String guid = "7c8f8c2c-cc48-429e-8a21-a1f1851ccdb0";
        final String name            = "ExternalId";
        final String description     = "Alternative identifier used in another system.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getExternalIdScopeRelationship()
    {
        final String guid = "8c5b1415-2d1f-4190-ba6c-1fdd47f03269";
        final String name            = "ExternalIdScope";
        final String description     = "Places where an external identifier is recognized.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getExternalIdLinkRelationship()
    {
        final String guid = "28ab0381-c662-4b6d-b787-5d77208de126";
        final String name            = "ExternalIdLink";
        final String description     = "Link between an external identifier and an asset.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EntityDef  getPropertyFacetEntity()
    {
        final String guid = "6403a704-aad6-41c2-8e08-b9525c006f85";
        final String name            = "PropertyFacet";
        final String description     = "Additional properties that support a particular vendor or service.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getReferenceableFacetRelationship()
    {
        final String guid = "58c87647-ada9-4c90-a3c3-a40ace46b1f7";
        final String name            = "ReferenceableFacet";
        final String description     = "Link between a property facet and the element it relates to.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EntityDef  getLocationEntity()
    {
        final String guid = "3e09cb2b-5f15-4fd2-b004-fe0146ad8628";
        final String name            = "Location";
        final String description     = "A physical place, digital location or area.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getNestedLocationRelationship()
    {
        final String guid = "f82a96c2-95a3-4223-88c0-9cbf2882b772";
        final String name            = "NestedLocation";
        final String description     = "Link between two locations to show one is nested inside another.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getAdjacentLocationRelationship()
    {
        final String guid = "017d0518-fc25-4e5e-985e-491d91e61e17";
        final String name            = "AdjacentLocation";
        final String description     = "Link between two locations that are next to one another.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getAssetLocationRelationship()
    {
        final String guid = "bc236b62-d0e6-4c5c-93a1-3a35c3dba7b1";
        final String name            = "AssetLocation";
        final String description     = "Location of an Asset.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private ClassificationDef  getMobileAssetClassification()
    {
        final String guid = "b25fb90d-8fa2-4aa9-b884-ff0a6351a697";
        final String name            = "MobileAsset";
        final String description     = "An asset not restricted to a single physical location.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private ClassificationDef  getFixedLocationClassification()
    {
        final String guid = "bc111963-80c7-444f-9715-946c03142dd2";
        final String name            = "FixedLocation";
        final String description     = "A location linked to a physical place.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private ClassificationDef  getSecureLocationClassification()
    {
        final String guid = "e7b563c0-fcdd-4ba7-a046-eecf5c4638b8";
        final String name            = "SecureLocation";
        final String description     = "A location that protects the assets in its care.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private ClassificationDef  getCyberLocationClassification()
    {
        final String guid = "f9ec3633-8ac8-480b-aa6d-5e674b9e1b17";
        final String name            = "CyberLocation";
        final String description     = "A digital location.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EnumDef  getEndiannessEnum()
    {
        final String guid = "e5612c3a-49bd-4148-8f67-cfdf145d5fd8";
        final String name = "Endianness";
        final String description = "Defines the sequential order in which bytes are arranged into larger numerical values when stored in memory or when transmitted over digital links.";
        final String descriptionGUID = null;

        EnumDef enumDef = getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs    = new ArrayList();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Big Endian";
        final String element1Description     = "Bits or bytes order from the big end.";
        final String element1DescriptionGUID = null;

        elementDef = getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Little Endian";
        final String element2Description     = "Bits or bytes ordered from the little end.";
        final String element2DescriptionGUID = null;

        elementDef = getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        return enumDef;
    }

    private EntityDef  getITInfrastructureEntity()
    {
        final String guid = "151e6dd1-54a0-4b7f-a072-85caa09d1dda";
        final String name            = "ITInfrastructure";
        final String description     = "Hardware and base software that supports an IT system.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private EntityDef  getHostEntity()
    {
        final String guid = "1abd16db-5b8a-4fd9-aee5-205db3febe99";
        final String name            = "Host";
        final String description     = "Named IT infrastructure system that supports multiple software servers.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private EntityDef  getOperatingPlatformEntity()
    {
        final String guid = "bd96a997-8d78-42f6-adf7-8239bc98501c";
        final String name            = "OperatingPlatform";
        final String description     = "Characteristics of the operating system in use within a host.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getHostLocationRelationship()
    {
        final String guid = "f3066075-9611-4886-9244-32cc6eb07ea9";
        final String name            = "HostLocation";
        final String description     = "Defines the location of a host.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getHostOperatingPlatformRelationship()
    {
        final String guid = "b9179df5-6e23-4581-a8b0-2919e6322b12";
        final String name            = "HostOperatingPlatform";
        final String description     = "Identifies the operating platform for a host.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EntityDef  getHostClusterEntity()
    {
        final String guid = "9794f42f-4c9f-4fe6-be84-261f0a7de890";
        final String name            = "HostCluster";
        final String description     = "A group of hosts operating together to provide a scalable platform.";
        final String descriptionGUID = null;
        // TODO
        return null;
    }

    private EntityDef  getVirtualContainerEntity()
    {
        final String guid = "e2393236-100f-4ac0-a5e6-ce4e96c521e7";
        final String name            = "VirtualContainer";
        final String description     = "Container-based virtual host.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getHostClusterMemberRelationship()
    {
        final String guid = "1a1c3933-a583-4b0c-9e42-c3691296a8e0";
        final String name            = "HostClusterMember";
        final String description     = "Identifies a host as a member of a host cluster.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getDeployedVirtualContainerRelationship()
    {
        final String guid = "4b981d89-e356-4d9b-8f17-b3a8d5a86676";
        final String name            = "DeployedVirtualContainer";
        final String description     = "Identifies the real host where a virtual container is deployed to.";
        final String descriptionGUID = null;

        // TODO
        return null;
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


    private EnumDef  getOperationalStatusEnum()
    {
        final String guid = "24e1e33e-9250-4a6c-8b07-05c7adec3a1d";
        final String name = "OperationalStatus";
        final String description = "Defines whether a component is operational.";
        final String descriptionGUID = null;

        EnumDef enumDef = getEmptyEnumDef(guid, name, description, descriptionGUID);

        ArrayList<EnumElementDef> elementDefs    = new ArrayList();
        EnumElementDef            elementDef;

        final int    element1Ordinal         = 0;
        final String element1Value           = "Disabled";
        final String element1Description     = "The component is not operational.";
        final String element1DescriptionGUID = null;

        elementDef = getEnumElementDef(element1Ordinal, element1Value, element1Description, element1DescriptionGUID);
        elementDefs.add(elementDef);

        final int    element2Ordinal         = 1;
        final String element2Value           = "Enabled";
        final String element2Description     = "The component is operational.";
        final String element2DescriptionGUID = null;

        elementDef = getEnumElementDef(element2Ordinal, element2Value, element2Description, element2DescriptionGUID);
        elementDefs.add(elementDef);

        return enumDef;
    }


    private EntityDef  getSoftwareServerEntity()
    {
        final String guid = "aa7c7884-32ce-4991-9c41-9778f1fec6aa";
        final String name            = "SoftwareServer";
        final String description     = "Software services to support a runtime environment for applications and data stores.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }


    private EntityDef  getEndpointEntity()
    {
        final String guid = "dbc20663-d705-4ff0-8424-80c262c6b8e7";
        final String name            = "Endpoint";
        final String description     = "Description of the network address and related information needed to call a software service.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }


    private EntityDef  getSoftwareServerCapabilityEntity()
    {
        final String guid = "fe30a033-8f86-4d17-8986-e6166fa24177";
        final String name            = "SoftwareServerCapability";
        final String description     = "A software capability such as an application, that is deployed to a software server.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }


    private RelationshipDef  getServerDeploymentRelationship()
    {
        final String guid = "d909eb3b-5205-4180-9f63-122a65b30738";
        final String name            = "ServerDeployment";
        final String description     = "Defines the host that a software server is deployed to.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getServerSupportedCapabilityRelationship()
    {
        final String guid = "8b7d7da5-0668-4174-a43b-8f8c6c068dd0";
        final String name            = "ServerSupportedCapability";
        final String description     = "Identifies a software capability that is deployed to a software server.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getServerEndpointRelationship()
    {
        final String guid = "2b8bfab4-8023-4611-9833-82a0dc95f187";
        final String name            = "ServerEndpoint";
        final String description     = "Defines an endpoint associated with a server.";
        final String descriptionGUID = null;

        // TODO
        return null;
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


    private EnumDef  getServerAssetUseTypeEnum()
    {
        final String guid = "09439481-9489-467c-9ae5-178a6e0b6b5a";
        final String name            = "ServerAssetUse";
        final String description     = "Defines how a server capability may use an asset.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private RelationshipDef  getServerAssetUseRelationship()
    {
        final String guid = "92b75926-8e9a-46c7-9d98-89009f622397";
        final String name            = "AssetServerUse";
        final String description     = "Defines that a server capability is using an asset.";
        final String descriptionGUID = null;

        // TODO
        return null;
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

    private EntityDef  getNetworkEntity()
    {
        final String guid = "e0430f59-f021-411a-9d81-883e1ff3f6f6";
        final String name            = "ITInfrastructure";
        final String description     = "Hardware and base software that supports an IT system.";
        final String descriptionGUID = null;

        // TODO
        return null;
    }

    private EntityDef  getNetworkGatewayEntity()
    {
        final String guid = "9bbae94d-e109-4c96-b072-4f97123f04fd";

        // TODO
        return null;
    }

    private RelationshipDef  getHostNetworkRelationship()
    {
        final String guid = "f2bd7401-c064-41ac-862c-e5bcdc98fa1e";

        // TODO
        return null;
    }


    private RelationshipDef  getNetworkGatewayLinkRelationship()
    {
        final String guid = "5bece460-1fa6-41fb-a29f-fdaf65ec8ce3";

        // TODO
        return null;
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

    private ClassificationDef  getCloudProviderClassification()
    {
        final String guid = "a2bfdd08-d0a8-49db-bc97-7f2406281046";

        // TODO
        return null;
    }

    private ClassificationDef  getCloudPlatformClassification()
    {
        final String guid = "1b8f8511-e606-4f65-86d3-84891706ad12";

        // TODO
        return null;
    }

    private ClassificationDef  getCloudServiceClassification()
    {
        final String guid = "337e7b1a-ad4b-4818-aa3e-0ff3307b2fbe";

        // TODO
        return null;
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

    private EnumDef  getCrowdSourcingRoleEnum()
    {
        final String guid = "0ded50c2-17cc-4ecf-915e-908e66dbb27f";

        // TODO
        return null;
    }

    private EnumDef  getContactMethodTypeEnum()
    {
        final String guid = "30e7d8cd-df01-46e8-9247-a24c5650910d";

        // TODO
        return null;
    }

    private EntityDef  getActorProfileEntity()
    {
        final String guid = "5a2f38dc-d69d-4a6f-ad26-ac86f118fa35";

        // TODO
        return null;
    }

    private EntityDef  getTeamEntity()
    {
        final String guid = "36db26d5-aba2-439b-bc15-d62d373c5db6";

        // TODO
        return null;
    }

    private EntityDef  getPersonEntity()
    {
        final String guid = "ac406bf8-e53e-49f1-9088-2af28bbbd285";

        // TODO
        return null;
    }

    private EntityDef  getUserIdentityEntity()
    {
        final String guid = "fbe95779-1f3c-4ac6-aa9d-24963ff16282";

        // TODO
        return null;
    }

    private EntityDef  getContactDetailsEntity()
    {
        final String guid = "79296df8-645a-4ef7-a011-912d1cdcf75a";

        // TODO
        return null;
    }

    private RelationshipDef  getContactThroughRelationship()
    {
        final String guid = "6cb9af43-184e-4dfa-854a-1572bcf0fe75";

        // TODO
        return null;
    }

    private RelationshipDef  getLeadershipRelationship()
    {
        final String guid = "5ebc4fb2-b62a-4269-8f18-e9237a2119ca";

        // TODO
        return null;
    }

    private RelationshipDef  getPeerRelationship()
    {
        final String guid = "4a316abe-bccd-4d11-ad5a-4bfb4079b80b";

        // TODO
        return null;
    }

    private RelationshipDef  getProfileIdentityRelationship()
    {
        final String guid = "01664609-e777-4079-b543-6baffe910ff1";

        // TODO
        return null;
    }

    private RelationshipDef  getContributorRelationship()
    {
        final String guid = "4db83564-b200-4956-94a4-c95a5c30e65a";

        // TODO
        return null;
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


    private EnumDef  getOrderByEnum()
    {
        final String guid = "1d412439-4272-4a7e-a940-1065f889fc56";

        // TODO
        return null;
    }

    private EntityDef  getCollectionEntity()
    {
        final String guid = "347005ba-2b35-4670-b5a7-12c9ebed0cf7";

        // TODO
        return null;
    }

    private RelationshipDef  getCollectionMembershipRelationship()
    {
        final String guid = "5cabb76a-e25b-4bb5-8b93-768bbac005af";

        // TODO
        return null;
    }

    private RelationshipDef  getActorCollectionRelationship()
    {
        final String guid = "73cf5658-6a73-4ebc-8f4d-44fdfac0b437";

        // TODO
        return null;
    }

    private ClassificationDef  getFolderClassification()
    {
        final String guid = "3c0fa687-8a63-4c8e-8bda-ede9c78be6c7";

        // TODO
        return null;
    }

    private ClassificationDef  getSetClassification()
    {
        final String guid = "3947f08d-7412-4022-81fc-344a20dfbb26";

        // TODO
        return null;
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

    private EntityDef  getProjectEntity()
    {
        final String guid = "0799569f-0c16-4a1f-86d9-e2e89568f7fd";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectHierarchyRelationship()
    {
        final String guid = "8f1134f6-b9fe-4971-bc57-6e1b8b302b55";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectDependencyRelationship()
    {
        final String guid = "5b6a56f1-68e2-4e10-85f0-fda47a4263fd";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectTeamRelationship()
    {
        final String guid = "746875af-2e41-4d1f-864b-35265df1d5dc";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectResourcesRelationship()
    {
        final String guid = "03d25e7b-1c5b-4352-a472-33aa0ddcad4d";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectScopeRelationship()
    {
        final String guid = "bc63ac45-b4d0-4fba-b583-92859de77dd8";

        // TODO
        return null;
    }

    private ClassificationDef  getTaskClassification()
    {
        final String guid = "2312b668-3670-4845-a140-ef88d5a6db0c";

        // TODO
        return null;
    }

    private ClassificationDef  getCampaignClassification()
    {
        final String guid = "41437629-8609-49ef-8930-8c435c912572";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0135 Meetings defines how to record meetings and todos.
     */
    private void add0135Meetings()
    {
        this.archiveBuilder.addEnumDef(getToDoEnum());

        this.archiveBuilder.addEntityDef(getMeetingEntity());
        this.archiveBuilder.addEntityDef(getToDoEntity());

        this.archiveBuilder.addRelationshipDef(getMeetingOnReferenceableRelationship());
        this.archiveBuilder.addRelationshipDef(getTodoOnReferenceableRelationship());
        this.archiveBuilder.addRelationshipDef(getProjectMeetingRelationship());
    }


    private EnumDef  getToDoEnum()
    {
        final String guid = "7197ea39-334d-403f-a70b-d40231092df7";

        // TODO
        return null;
    }

    private EntityDef  getMeetingEntity()
    {
        final String guid = "6bf90c79-32f4-47ad-959c-8fff723fe744";

        // TODO
        return null;
    }

    private EntityDef  getToDoEntity()
    {
        final String guid = "93dbc58d-c826-4bc2-b36f-195148d46f86";

        // TODO
        return null;
    }

    private RelationshipDef  getMeetingOnReferenceableRelationship()
    {
        final String guid = "a05f918e-e7e2-419d-8016-5b37406df63a";

        // TODO
        return null;
    }

    private RelationshipDef  getTodoOnReferenceableRelationship()
    {
        final String guid = "aca1277b-bf1c-42f5-9b3b-fbc2c9047325";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectMeetingRelationship()
    {
        final String guid = "af2b5fab-8f83-4a2b-b749-1e6219f61f79";

        // TODO
        return null;
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


    private EnumDef  getCommunityMembershipTypeEnum()
    {
        final String guid = "b0ef45bf-d12b-4b6f-add6-59c14648d750";

        // TODO
        return null;
    }

    private EntityDef  getCommunityEntity()
    {
        final String guid = "fbd42379-f6c3-4f08-b6f7-378565cda993";

        // TODO
        return null;
    }

    private RelationshipDef  getCommunityMembershipRelationship()
    {
        final String guid = "7c7da1a3-01b3-473e-972e-606eff0cb112";

        // TODO
        return null;
    }

    private RelationshipDef  getCommunityResourcesRelationship()
    {
        final String guid = "484d4fb9-4927-4926-8e6d-03e6c9885254";

        // TODO
        return null;
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

        this.archiveBuilder.addRelationshipDef(getAttachRatingRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedCommentRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedLikeRelationship());
        this.archiveBuilder.addRelationshipDef(getAcceptedAnswerRelationship());
        this.archiveBuilder.addRelationshipDef(getAttachedTagRelationship());
    }


    private EnumDef  getStarRatingEnum()
    {
        final String guid = "77fea3ef-6ec1-4223-8408-38567e9d3c93";

        // TODO
        return null;
    }

    private EnumDef  getCommentTypeEnum()
    {
        final String guid = "06d5032e-192a-4f77-ade1-a4b97926e867";

        // TODO
        return null;
    }

    private EntityDef  getRatingEntity()
    {
        final String guid = "7299d721-d17f-4562-8286-bcd451814478";

        // TODO
        return null;
    }

    private EntityDef  getCommentEntity()
    {
        final String guid = "1a226073-9c84-40e4-a422-fbddb9b84278";

        // TODO
        return null;
    }

    private EntityDef  getLikeEntity()
    {
        final String guid = "deaa5ca0-47a0-483d-b943-d91c76744e01";

        // TODO
        return null;
    }

    private EntityDef  getInformalTagEntity()
    {
        final String guid = "ba846a7b-2955-40bf-952b-2793ceca090a";

        // TODO
        return null;
    }

    private EntityDef  getPrivateTagEntity()
    {
        final String guid = "9b3f5443-2475-4522-bfda-8f1f17e9a6c3";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachRatingRelationship()
    {
        final String guid = "0aaad9e9-9cc5-4ad8-bc2e-c1099bab6344";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachedCommentRelationship()
    {
        final String guid = "0d90501b-bf29-4621-a207-0c8c953bdac9";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachedLikeRelationship()
    {
        final String guid = "e2509715-a606-415d-a995-61d00503dad4";

        // TODO
        return null;
    }

    private RelationshipDef  getAcceptedAnswerRelationship()
    {
        final String guid = "ecf1a3ca-adc5-4747-82cf-10ec590c5c69";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachedTagRelationship()
    {
        final String guid = "4b1641c4-3d1a-4213-86b2-d6968b6c65ab";

        // TODO
        return null;
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

    private EntityDef  getNoteEntryEntity()
    {
        final String guid = "2a84d94c-ac6f-4be1-a72a-07dcec7b1fe3";

        // TODO
        return null;
    }

    private EntityDef  getNoteLogEntity()
    {
        final String guid = "646727c7-9ad4-46fa-b660-265489ad96c6";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachedNoteLogRelationship()
    {
        final String guid = "4f798c0c-6769-4a2d-b489-d2714d89e0a4";

        // TODO
        return null;
    }

    private RelationshipDef  getAttachedNoteLogEntryRelationship()
    {
        final String guid = "38edecc6-f385-4574-8144-524a44e3e712";

        // TODO
        return null;
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
        this.add0211DataSets();
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
        this.add0235InfomationView();
        this.add0237InformationSet();
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
    void add0201ConnectorsAndConnections()
    {
        this.archiveBuilder.addEntityDef(getConnectionEntity());
        this.archiveBuilder.addEntityDef(getConnectorTypeEntity());

        this.archiveBuilder.addRelationshipDef(getConnectionEndpointRelationship());
        this.archiveBuilder.addRelationshipDef(getConnectionConnectorTypeRelationship());
    }

    private EntityDef  getConnectionEntity()
    {
        final String guid = "114e9f8f-5ff3-4c32-bd37-a7eb42712253";

        // TODO
        return null;
    }

    private EntityDef  getConnectorTypeEntity()
    {
        final String guid = "954421eb-33a6-462d-a8ca-b5709a1bd0d4";

        // TODO
        return null;
    }

    private RelationshipDef  getConnectionEndpointRelationship()
    {
        final String guid = "887a7132-d6bc-4b92-a483-e80b60c86fb2";

        // TODO
        return null;
    }

    private RelationshipDef  getConnectionConnectorTypeRelationship()
    {
        final String guid = "e542cfc1-0b4b-42b9-9921-f0a5a88aaf96";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0205 Connection Links defines the relationship between the connection and an Asset, plus the nesting
     * of connections for information virtualization support.
     */
    void add0205ConnectionLinkage()
    {
        this.archiveBuilder.addEntityDef(getVirtualConnectionEntity());

        this.archiveBuilder.addRelationshipDef(getEmbeddedConnectionRelationship());
        this.archiveBuilder.addRelationshipDef(getConnectionsToAssetRelationship());
    }

    private EntityDef  getVirtualConnectionEntity()
    {
        final String guid = "82f9c664-e59d-484c-a8f3-17088c23a2f3";

        // TODO
        return null;
    }

    private RelationshipDef  getEmbeddedConnectionRelationship()
    {
        final String guid = "eb6dfdd2-8c6f-4f0d-a17d-f6ce4799f64f";

        // TODO
        return null;
    }

    private RelationshipDef  getConnectionsToAssetRelationship()
    {
        final String guid = "e777d660-8dbe-453e-8b83-903771f054c0";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0210 DataStores describe physical data store assets.
     */
    void add0210DataStores()
    {
        this.archiveBuilder.addEntityDef(getDataStoreEntity());

        this.archiveBuilder.addRelationshipDef(getPhysicalStoreForDataSetRelationship());

        this.archiveBuilder.addClassificationDef(getDataStoreEncodingClassification());
    }

    private EntityDef  getDataStoreEntity()
    {
        final String guid = "30756d0b-362b-4bfa-a0de-fce6a8f47b47";

        // TODO
        return null;
    }

    private RelationshipDef  getPhysicalStoreForDataSetRelationship()
    {
        final String guid = "b827683c-2924-4df3-a92d-7be1888e23c0";

        // TODO
        return null;
    }

    private ClassificationDef  getDataStoreEncodingClassification()
    {
        final String guid = "f08e48b5-6b66-40f5-8ff6-c2bfe527330b";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0211 Data Sets defines virtual data sets
     */
    void add0211DataSets()
    {
        this.archiveBuilder.addEntityDef(getVirtualDataSetEntity());

        this.archiveBuilder.addRelationshipDef(getVirtualDataSetDependencyRelationship());
    }

    private EntityDef  getVirtualDataSetEntity()
    {
        final String guid = "29ef8784-9cdf-42bf-b425-b65e264e1d1f";

        // TODO
        return null;
    }

    private RelationshipDef  getVirtualDataSetDependencyRelationship()
    {
        final String guid = "e11d4657-fff4-4df2-848f-ab1c38daf167";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0212 Deployed APIs defines an API that has been deployed to IT Infrastructure
     */
    void add0212DeployedAPIs()
    {
        this.archiveBuilder.addEntityDef(getDeployedAPIEntity());

        this.archiveBuilder.addRelationshipDef(getAPIEndpointRelationship());

        this.archiveBuilder.addClassificationDef(getRequestResponseInterfaceClassification());
        this.archiveBuilder.addClassificationDef(getListenerInterfaceClassification());
        this.archiveBuilder.addClassificationDef(getPublisherInterfaceClassification());
    }

    private EntityDef  getDeployedAPIEntity()
    {
        final String guid = "7dbb3e63-138f-49f1-97b4-66313871fc14";

        // TODO
        return null;
    }

    private RelationshipDef  getAPIEndpointRelationship()
    {
        final String guid = "de5b9501-3ad4-4803-a8b2-e311c72a4336";

        // TODO
        return null;
    }

    private ClassificationDef  getRequestResponseInterfaceClassification()
    {
        final String guid = "14a29330-e830-4343-a41e-d57e2cec82f8";

        // TODO
        return null;
    }

    private ClassificationDef  getListenerInterfaceClassification()
    {
        final String guid = "4099d2ed-2a5e-4c44-8443-9de4e378a4ba";

        // TODO
        return null;
    }

    private ClassificationDef  getPublisherInterfaceClassification()
    {
        final String guid = "4fdedcd5-b186-4bee-887a-02fa29a10750";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0215 Software Components defines a generic Asset for a software component.
     */
    void add0215SoftwareComponents()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0217 Automated Processes defines a Process is automated (as opposed to manual).
     */
    void add0217AutomatedProcesses()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0220 Files and Folders provides the definitions for describing filesystems and their content
     */
    void add0220FilesAndFolders()
    {
        this.archiveBuilder.addEntityDef(getFileFolderEntity());
        this.archiveBuilder.addEntityDef(getDataFileEntity());

        this.archiveBuilder.addRelationshipDef(getFolderHierarchyRelationship());
        this.archiveBuilder.addRelationshipDef(getNestedFileRelationship());
        this.archiveBuilder.addRelationshipDef(getLinkedFileRelationship());

        this.archiveBuilder.addClassificationDef(getFileSystemClassification());
    }


    private EntityDef  getFileFolderEntity()
    {
        final String guid = "229ed5cc-de31-45fc-beb4-9919fd247398";

        // TODO
        return null;
    }

    private EntityDef  getDataFileEntity()
    {
        final String guid = "10752b4a-4b5d-4519-9eae-fdd6d162122f";

        // TODO
        return null;
    }

    private RelationshipDef  getFolderHierarchyRelationship()
    {
        final String guid = "48ac9028-45dd-495d-b3e1-622685b54a01";

        // TODO
        return null;
    }

    private RelationshipDef  getNestedFileRelationship()
    {
        final String guid = "4cb88900-1446-4eb6-acea-29cd9da45e63";

        // TODO
        return null;
    }

    private RelationshipDef  getLinkedFileRelationship()
    {
        final String guid = "970a3405-fde1-4039-8249-9aa5f56d5151";

        // TODO
        return null;
    }

    private ClassificationDef  getFileSystemClassification()
    {
        final String guid = "cab5ba1d-cfd3-4fca-857d-c07711fc4157";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0221 Document Stores define both simple document stores and content management systems
     */
    void add0221DocumentStores()
    {
        this.archiveBuilder.addEntityDef(getDocumentEntity());
        this.archiveBuilder.addEntityDef(getDocumentCollectionEntity());

        this.archiveBuilder.addRelationshipDef(getRelatedDocumentsRelationship());

        this.archiveBuilder.addClassificationDef(getContentManagerClassification());
        this.archiveBuilder.addClassificationDef(getDocumentStoreClassification());
    }

    private EntityDef  getDocumentEntity()
    {
        final String guid = "b463827c-c0a0-4cfb-a2b2-ddc63746ded4";

        // TODO
        return null;
    }

    private EntityDef  getDocumentCollectionEntity()
    {
        final String guid = "0075d603-1627-41c5-8cae-f5458d1247fe";

        // TODO
        return null;
    }

    private RelationshipDef  getRelatedDocumentsRelationship()
    {
        final String guid = "7d881574-461d-475c-ab44-077451528cb8";

        // TODO
        return null;
    }


    private ClassificationDef  getContentManagerClassification()
    {
        final String guid = "fa4df7b5-cb6d-475c-889e-8f3b7ca564d3";

        // TODO
        return null;
    }

    private ClassificationDef  getDocumentStoreClassification()
    {
        final String guid = "37156790-feac-4e1a-a42e-88858ae6f8e1";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0222GraphStores()
    {
        this.archiveBuilder.addClassificationDef(getGraphStoreClassification());
    }

    private ClassificationDef  getGraphStoreClassification()
    {
        final String guid = "86de3633-eec8-4bf9-aad1-e92df1ca2024";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0223 Events and Logs describes events, log files and event management.
     */
    void add0223EventsAndLogs()
    {
        this.archiveBuilder.addEntityDef(getSubscriberListEntity());
        this.archiveBuilder.addEntityDef(getTopicEntity());

        this.archiveBuilder.addRelationshipDef(getTopicSubcribersRelationship());

        this.archiveBuilder.addClassificationDef(getLogFileClassification());
        this.archiveBuilder.addClassificationDef(getNotificationManagerClassification());
    }

    private EntityDef  getSubscriberListEntity()
    {
        final String guid = "69751093-35f9-42b1-944b-ba6251ff513d";

        // TODO
        return null;
    }

    private EntityDef  getTopicEntity()
    {
        final String guid = "29100f49-338e-4361-b05d-7e4e8e818325";

        // TODO
        return null;
    }

    private RelationshipDef  getTopicSubcribersRelationship()
    {
        final String guid = "bc91a28c-afb9-41a7-8eb2-fc8b5271fe9e";

        // TODO
        return null;
    }


    private ClassificationDef  getLogFileClassification()
    {
        final String guid = "ff4c8484-9127-464a-97fc-99579d5bc429";

        // TODO
        return null;
    }

    private ClassificationDef  getNotificationManagerClassification()
    {
        final String guid = "3e7502a7-396a-4737-a106-378c9c94c105";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0224 Databases describe database servers
     */
    void add0224Databases()
    {
        this.archiveBuilder.addEntityDef(getDeployedDatabaseSchemaEntity());

        this.archiveBuilder.addClassificationDef(getDatabaseClassification());
        this.archiveBuilder.addClassificationDef(getDatabaseServerClassification());
    }


    private EntityDef  getDeployedDatabaseSchemaEntity()
    {
        final String guid = "eab811ec-556a-45f1-9091-bc7ac8face0f";

        // TODO
        return null;
    }

    private ClassificationDef  getDatabaseClassification()
    {
        final String guid = "0921c83f-b2db-4086-a52c-0d10e52ca078";

        // TODO
        return null;
    }

    private ClassificationDef  getDatabaseServerClassification()
    {
        final String guid = "6bb58cc9-ed9e-4f75-b2f2-6d308554eb52";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0225MetadataRepositories()
    {
        this.archiveBuilder.addEntityDef(getEnterpriseAccessLayerEntity());
        this.archiveBuilder.addEntityDef(getCohortRegistryEntity());
        this.archiveBuilder.addEntityDef(getMetadataRepositoryCohortEntity());
        this.archiveBuilder.addEntityDef(getMetadataCollectionEntity());

        this.archiveBuilder.addRelationshipDef(getMetadataCohortPeerRelationship());

        this.archiveBuilder.addClassificationDef(getMetadataServerClassification());
        this.archiveBuilder.addClassificationDef(getRepositoryProxyClassification());
        this.archiveBuilder.addClassificationDef(getMetadataRepositoryClassification());
        this.archiveBuilder.addClassificationDef(getCohortRegistryStoreClassification());
    }

    private EntityDef  getEnterpriseAccessLayerEntity()
    {
        final String guid = "39444bf9-638e-4124-a5f9-1b8f3e1b008b";

        // TODO
        return null;
    }

    private EntityDef  getCohortRegistryEntity()
    {
        final String guid = "42063797-a78a-4720-9353-52026c75f667";

        // TODO
        return null;
    }

    private EntityDef  getMetadataRepositoryCohortEntity()
    {
        final String guid = "43e7dca2-c7b4-4cdf-a1ea-c9d4f7093893";

        // TODO
        return null;
    }

    private EntityDef  getMetadataCollectionEntity()
    {
        final String guid = "ea3b15af-ed0e-44f7-91e4-bdb299dd4976";

        // TODO
        return null;
    }

    private RelationshipDef  getMetadataCohortPeerRelationship()
    {
        final String guid = "954cdba1-3d69-4db1-bf0e-d59fd2c25a27";

        // TODO
        return null;
    }

    private ClassificationDef  getMetadataServerClassification()
    {
        final String guid = "74a256ad-4022-4518-a446-c65fe082d4d3";

        // TODO
        return null;
    }

    private ClassificationDef  getRepositoryProxyClassification()
    {
        final String guid = "ae81c35e-7078-46f0-9b2c-afc99accf3ec";

        // TODO
        return null;
    }

    private ClassificationDef  getMetadataRepositoryClassification()
    {
        final String guid = "c40397bd-eab0-4b2e-bffb-e7fa0f93a5a9";

        // TODO
        return null;
    }

    private ClassificationDef  getCohortRegistryStoreClassification()
    {
        final String guid = "2bfdcd0d-68bb-42c3-ae75-e9fb6c3dff70";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0227Keystores()
    {
        this.archiveBuilder.addEntityDef(getKeystoreFileEntity());
        this.archiveBuilder.addEntityDef(getKeystoreCollectionEntity());
    }

    private EntityDef  getKeystoreFileEntity()
    {
        final String guid = "17bee904-5b35-4c81-ac63-871c615424a2";

        // TODO
        return null;
    }

    private EntityDef  getKeystoreCollectionEntity()
    {
        final String guid = "979d97dd-6782-4648-8e2a-8982994533e6";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0230CodeTables()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0235InfomationView()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0237InformationSet()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0239Reports()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0240ApplicationsAndProcesses()
    {
        this.archiveBuilder.addEntityDef(getApplicationEntity());

        this.archiveBuilder.addRelationshipDef(getRuntimeForProcessRelationship());

        this.archiveBuilder.addClassificationDef(getApplicationServerClassification());
        this.archiveBuilder.addClassificationDef(getWebserverClassification());
    }

    private EntityDef  getApplicationEntity()
    {
        final String guid = "58280f3c-9d63-4eae-9509-3f223872fb25";

        // TODO
        return null;
    }

    private RelationshipDef  getRuntimeForProcessRelationship()
    {
        final String guid = "f6b5cf4f-7b88-47df-aeb0-d80d28ba1ec1";

        // TODO
        return null;
    }

    private ClassificationDef  getApplicationServerClassification()
    {
        final String guid = "19196efb-2706-47bf-8e51-e8ba5b36d033";

        // TODO
        return null;
    }

    private ClassificationDef  getWebserverClassification()
    {
        final String guid = "d13e1cc5-bb7e-41ec-8233-9647fbf92a19";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0250DataProcessingEngines()
    {
        this.archiveBuilder.addEntityDef(getEngineProfileEntity());
        this.archiveBuilder.addEntityDef(getEngineEntity());

        this.archiveBuilder.addClassificationDef(getWorkflowEngineClassification());
        this.archiveBuilder.addClassificationDef(getReportingEngineClassification());
        this.archiveBuilder.addClassificationDef(getAnalyticsEngineClassification());
        this.archiveBuilder.addClassificationDef(getDataMovementEngineClassification());
    }

    private EntityDef  getEngineProfileEntity()
    {
        final String guid = "81394f85-6008-465b-926e-b3fae4668937";

        // TODO
        return null;
    }

    private EntityDef  getEngineEntity()
    {
        final String guid = "3566527f-b1bd-4e7a-873e-a3e04d5f2a14";

        // TODO
        return null;
    }

    private ClassificationDef  getWorkflowEngineClassification()
    {
        final String guid = "37a6d212-7c4a-4a82-b4e2-601d4358381c";

        // TODO
        return null;
    }

    private ClassificationDef  getReportingEngineClassification()
    {
        final String guid = "e07eefaa-16e0-46cf-ad54-bed47fb15812";

        // TODO
        return null;
    }

    private ClassificationDef  getAnalyticsEngineClassification()
    {
        final String guid = "1a0dc6f6-7980-42f5-98bd-51e56543a07e";

        // TODO
        return null;
    }

    private ClassificationDef  getDataMovementEngineClassification()
    {
        final String guid = "d2ed6621-9d99-4fe8-843a-b28d816cf888";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0260Transformations()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0265AnalyticsAssets()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0270IoTAssets()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0280ModelAssets()
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

        this.archiveBuilder.addClassificationDef(getIsTaxonomyClassification());
        this.archiveBuilder.addClassificationDef(getIsCanonicalVocabularyClassification());
    }

    private EntityDef  getGlossaryEntity()
    {
        final String guid = "36f66863-9726-4b41-97ee-714fd0dc6fe4";

        // TODO
        return null;
    }

    private EntityDef  getExternalGlossaryLinkEntity()
    {
        final String guid = "183d2935-a950-4d74-b246-eac3664b5a9d";

        // TODO
        return null;
    }

    private RelationshipDef  getExternalSourcedGlossaryRelationship()
    {
        final String guid = "7786a39c-436b-4538-acc7-d595b5856add";

        // TODO
        return null;
    }

    private ClassificationDef  getIsTaxonomyClassification()
    {
        final String guid = "37116c51-e6c9-4c37-942e-35d48c8c69a0";

        // TODO
        return null;
    }

    private ClassificationDef  getIsCanonicalVocabularyClassification()
    {
        final String guid = "33ad3da2-0910-47be-83f1-daee018a4c05";

        // TODO
        return null;
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

    private EntityDef  getGlossaryCategoryEntity()
    {
        final String guid = "e507485b-9b5a-44c9-8a28-6967f7ff3672";

        // TODO
        return null;
    }

    private RelationshipDef  getCategoryAnchorRelationship()
    {
        final String guid = "c628938e-815e-47db-8d1c-59bb2e84e028";

        // TODO
        return null;
    }

    private RelationshipDef  getCategoryHierarchyLinkRelationship()
    {
        final String guid = "71e4b6fb-3412-4193-aff3-a16eccd87e8e";

        // TODO
        return null;
    }

    private RelationshipDef  getLibraryCategoryReferenceRelationship()
    {
        final String guid = "3da21cc9-3cdc-4d87-89b5-c501740f00b2";

        // TODO
        return null;
    }

    private ClassificationDef  getSubjectAreaClassification()
    {
        final String guid = "480e6993-35c5-433a-b50b-0f5c4063fb5d";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0330 Terms brings in hte glossary term that captures a single semantic meaning.
     */
    private void add0330Terms()
    {
        this.archiveBuilder.addEntityDef(getGlossaryTermEntity());

        this.archiveBuilder.addRelationshipDef(getTermAnchorRelationship());
        this.archiveBuilder.addRelationshipDef(getTermCategorizationLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getLibraryTermReferenceRelationship());
    }

    private EntityDef  getGlossaryTermEntity()
    {
        final String guid = "0db3e6ec-f5ef-4d75-ae38-b7ee6fd6ec0a";

        // TODO
        return null;
    }

    private RelationshipDef  getTermAnchorRelationship()
    {
        final String guid = "1d43d661-bdc7-4a91-a996-3239b8f82e56";

        // TODO
        return null;
    }

    private RelationshipDef  getTermCategorizationLinkRelationship()
    {
        final String guid = "696a81f5-ac60-46c7-b9fd-6979a1e7ad27";

        // TODO
        return null;
    }

    private RelationshipDef  getLibraryTermReferenceRelationship()
    {
        final String guid = "38c346e4-ddd2-42ef-b4aa-55d53c078d22";

        // TODO
        return null;
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
    }

    private ClassificationDef  getActivityDescriptionClassification()
    {
        final String guid = "317f0e52-1548-41e6-b90c-6ae5e6c53fed";

        // TODO
        return null;
    }

    private ClassificationDef  getAbstractConceptClassification()
    {
        final String guid = "9d725a07-4abf-4939-a268-419d200b69c2";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0350 Related Terms provides a selection of semantic relationships
     */
    private void add0350RelatedTerms()
    {
        this.archiveBuilder.addEnumDef(getActivityTypeEnum());

        this.archiveBuilder.addRelationshipDef(getRelatedTermRelationship());
        this.archiveBuilder.addRelationshipDef(getSynonymRelationship());
        this.archiveBuilder.addRelationshipDef(getAntonymRelationship());
        this.archiveBuilder.addRelationshipDef(getPreferredTermRelationship());
        this.archiveBuilder.addRelationshipDef(getReplacementTermRelationship());
        this.archiveBuilder.addRelationshipDef(getTranslationRelationship());
        this.archiveBuilder.addRelationshipDef(getISARelationship());
        this.archiveBuilder.addRelationshipDef(getValidValueRelationship());
    }

    private EnumDef  getActivityTypeEnum()
    {
        final String guid = "af7e403d-9865-4ebb-8c1a-1fd57b4f4bca";

        // TODO
        return null;
    }

    private RelationshipDef  getRelatedTermRelationship()
    {
        final String guid = "b1161696-e563-4cf9-9fd9-c0c76e47d063";

        // TODO
        return null;
    }

    private RelationshipDef  getSynonymRelationship()
    {
        final String guid = "74f4094d-dba2-4ad9-874e-d422b69947e2";

        // TODO
        return null;
    }

    private RelationshipDef  getAntonymRelationship()
    {
        final String guid = "ea5e126a-a8fa-4a43-bcfa-309a98aa0185";

        // TODO
        return null;
    }

    private RelationshipDef  getPreferredTermRelationship()
    {
        final String guid = "8ac8f9de-9cdd-4103-8a33-4cb204b78c2a";

        // TODO
        return null;
    }

    private RelationshipDef  getReplacementTermRelationship()
    {
        final String guid = "3bac5f35-328b-4bbd-bfc9-3b3c9ba5e0ed";

        // TODO
        return null;
    }

    private RelationshipDef  getTranslationRelationship()
    {
        final String guid = "6ae42e95-efc5-4256-bfa8-801140a29d2a";

        // TODO
        return null;
    }

    private RelationshipDef  getISARelationship()
    {
        final String guid = "50fab7c7-68bc-452f-b8eb-ec76829cac85";

        // TODO
        return null;
    }

    private RelationshipDef  getValidValueRelationship()
    {
        final String guid = "707a156b-e579-4482-89a5-de5889da1971";

        // TODO
        return null;
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

    private RelationshipDef  getUsedInContextRelationship()
    {
        final String guid = "2dc524d2-e29f-4186-9081-72ea956c75de";

        // TODO
        return null;
    }

    private ClassificationDef  getContextDefinitionClassification()
    {
        final String guid = "54f9f41a-3871-4650-825d-59a41de01330";

        // TODO
        return null;
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

    private EnumDef  getTermAssignmentStatusEnum()
    {
        final String guid = "c8fe36ac-369f-4799-af75-46b9c1343ab3";

        // TODO
        return null;
    }

    private RelationshipDef  getSemanticAssignmentRelationship()
    {
        final String guid = "e6670973-645f-441a-bec7-6f5570345b92";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0380 Spine Objects enables relationships to be established between objects and their attributes.
     */
    private void add0380SpineObjects()
    {
        this.archiveBuilder.addRelationshipDef(getTermHASARelationship());
        this.archiveBuilder.addRelationshipDef(getTermISATYPEOFRelationship());
        this.archiveBuilder.addRelationshipDef(getTermTYPEDBYRelationship());

        this.archiveBuilder.addClassificationDef(getSpineObjectClassification());
        this.archiveBuilder.addClassificationDef(getSpineAttributeClassification());
        this.archiveBuilder.addClassificationDef(getObjectIdentifierClassification());
    }

    private RelationshipDef  getTermHASARelationship()
    {
        final String guid = "d67f16d1-5348-419e-ba38-b0bb6fe4ad6c";

        // TODO
        return null;
    }

    private RelationshipDef  getTermISATYPEOFRelationship()
    {
        final String guid = "d5d588c3-46c9-420c-adff-6031802a7e51";

        // TODO
        return null;
    }

    private RelationshipDef  getTermTYPEDBYRelationship()
    {
        final String guid = "669e8aa4-c671-4ee7-8d03-f37d09b9d006";

        // TODO
        return null;
    }


    private ClassificationDef  getSpineObjectClassification()
    {
        final String guid = "a41ee152-de1e-4533-8535-2f8b37897cac";

        // TODO
        return null;
    }

    private ClassificationDef  getSpineAttributeClassification()
    {
        final String guid = "ccb749ba-34ec-4f71-8755-4d8b383c34c3";

        // TODO
        return null;
    }

    private ClassificationDef  getObjectIdentifierClassification()
    {
        final String guid = "3d1e4389-27de-44fa-8df4-d57bfaf809ea";

        // TODO
        return null;
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

    private EntityDef  getControlledGlossaryTermEntity()
    {
        final String guid = "c04e29b2-2d66-48fc-a20d-e59895de6040";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0390 Glossary Project priveds a classification for a project to say it is updating glossary terms.
     */
    private void add0390GlossaryProject()
    {
        this.archiveBuilder.addClassificationDef(getGlossaryProjectClassification());
    }

    private ClassificationDef  getGlossaryProjectClassification()
    {
        final String guid = "43be51a9-2d19-4044-b399-3ba36af10929";

        // TODO
        return null;
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
        this.add0457SecurityCapabilities();
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
    void add0401GovernanceDefinitions()
    {
        this.archiveBuilder.addEntityDef(getGovernanceDefinitionEntity());
    }

    private EntityDef  getGovernanceDefinitionEntity()
    {
        final String guid = "578a3500-9ad3-45fe-8ada-e4e9572c37c8";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0405 Governance Drivers defines the factors that drive the governance program.
     */
    void add0405GovernanceDrivers()
    {
        this.archiveBuilder.addEntityDef(getGovernanceDriverEntity());
        this.archiveBuilder.addEntityDef(getDataStrategyEntity());
        this.archiveBuilder.addEntityDef(getRegulationEntity());
    }

    private EntityDef  getGovernanceDriverEntity()
    {
        final String guid = "c403c109-7b6b-48cd-8eee-df445b258b33";

        // TODO
        return null;
    }

    private EntityDef  getDataStrategyEntity()
    {
        final String guid = "3c34f121-07a6-4e95-a07d-9b0ef17b7bbf";

        // TODO
        return null;
    }

    private EntityDef  getRegulationEntity()
    {
        final String guid = "e3c4293d-8846-4500-b0c0-197d73aba8b0";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0415 Governance Responses lay out the approaches, principles and obligations that follow from the
     * governance drivers.
     */
    void add0415GovernanceResponses()
    {
        this.archiveBuilder.addEntityDef(getGovernancePolicyEntity());
        this.archiveBuilder.addEntityDef(getGovernancePrincipleEntity());
        this.archiveBuilder.addEntityDef(getGovernanceObligationEntity());
        this.archiveBuilder.addEntityDef(getGovernanceApproachEntity());

        this.archiveBuilder.addRelationshipDef(getGovernancePolicyLinkRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceResponseRelationship());
    }

    private EntityDef  getGovernancePolicyEntity()
    {
        final String guid = "a7defa41-9cfa-4be5-9059-359022bb016d";

        // TODO
        return null;
    }

    private EntityDef  getGovernancePrincipleEntity()
    {
        final String guid = "3b7d1325-ec2c-44cb-8db0-ce207beb78cf";

        // TODO
        return null;
    }

    private EntityDef  getGovernanceObligationEntity()
    {
        final String guid = "0cec20d3-aa29-41b7-96ea-1c544ed32537";

        // TODO
        return null;
    }

    private EntityDef  getGovernanceApproachEntity()
    {
        final String guid = "2d03ec9d-bd6b-4be9-8e17-95a7ecdbaa67";

        // TODO
        return null;
    }

    private RelationshipDef  getGovernancePolicyLinkRelationship()
    {
        final String guid = "0c42c999-4cac-4da4-afab-0e381f3a818e";

        // TODO
        return null;
    }

    private RelationshipDef  getGovernanceResponseRelationship()
    {
        final String guid = "8845990e-7fd9-4b79-a19d-6c4730dadd6b";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0417 Governance Project provides a classification for a project
     */
    void add0417GovernanceProject()
    {
        this.archiveBuilder.addClassificationDef(getGovernanceProjectClassification());
    }

    private ClassificationDef  getGovernanceProjectClassification()
    {
        final String guid = "37142317-4125-4046-9514-71dc5031563f";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0420 Governance Controls define the implementation of the governance policies
     */
    void add0420GovernanceControls()
    {
        this.archiveBuilder.addEntityDef(getGovernanceControlEntity());
        this.archiveBuilder.addEntityDef(getTechnicalControlEntity());
        this.archiveBuilder.addEntityDef(getOrganizationalControlEntity());

        this.archiveBuilder.addRelationshipDef(getGovernanceImplementationRelationship());
        this.archiveBuilder.addRelationshipDef(getGovernanceControlLinkRelationship());
    }

    private EntityDef  getGovernanceControlEntity()
    {
        final String guid = "c794985e-a10b-4b6c-9dc2-6b2e0a2901d3";

        // TODO
        return null;
    }

    private EntityDef  getTechnicalControlEntity()
    {
        final String guid = "d8f6eb5b-36f0-49bd-9b25-bf16f370d1ec";

        // TODO
        return null;
    }

    private EntityDef  getOrganizationalControlEntity()
    {
        final String guid = "befa1458-79b8-446a-b813-536700e60fa8";

        // TODO
        return null;
    }

    private RelationshipDef  getGovernanceImplementationRelationship()
    {
        final String guid = "787eaf46-7cf2-4096-8d6e-671a0819d57e";

        // TODO
        return null;
    }

    private RelationshipDef  getGovernanceControlLinkRelationship()
    {
        final String guid = "806933fb-7925-439b-9876-922a960d2ba1";

        // TODO
        return null;
    }



    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0422 Governance Action Classifications provide the key classification that drive information governance.
     */
    void add0422GovernanceActionClassifications()
    {
        this.archiveBuilder.addEnumDef(getGAClassificationStatusEnum());
        this.archiveBuilder.addEnumDef(getConfidentialityLevelEnum());
        this.archiveBuilder.addEnumDef(getConfidenceLevelEnum());
        this.archiveBuilder.addEnumDef(getRetentionBasisEnum());
        this.archiveBuilder.addEnumDef(getCriticalityLevelEnum());

        this.archiveBuilder.addClassificationDef(getGovernanceActionClassification());
        this.archiveBuilder.addClassificationDef(getConfidentialityClassification());
        this.archiveBuilder.addClassificationDef(getConfidenceClassification());
        this.archiveBuilder.addClassificationDef(getRetentionClassification());
        this.archiveBuilder.addClassificationDef(getCriticalityClassification());
    }

    private EnumDef  getGAClassificationStatusEnum()
    {
        final String guid = "cc540586-ac7c-41ba-8cc1-4da694a6a8e4";

        // TODO
        return null;
    }

    private EnumDef  getConfidentialityLevelEnum()
    {
        final String guid = "ecb48ca2-4d29-4de9-99a1-bc4db9816d68";

        // TODO
        return null;
    }

    private EnumDef  getConfidenceLevelEnum()
    {
        final String guid = "ae846797-d88a-4421-ad9a-318bf7c1fe6f";

        // TODO
        return null;
    }

    private EnumDef  getRetentionBasisEnum()
    {
        final String guid = "de79bf78-ecb0-4fd0-978f-ecc2cb4ff6c7";

        // TODO
        return null;
    }

    private EnumDef  getCriticalityLevelEnum()
    {
        final String guid = "22bcbf49-83e1-4432-b008-e09a8f842a1e";

        // TODO
        return null;
    }

    private ClassificationDef  getGovernanceActionClassification()
    {
        final String guid = "d3d62592-885e-4425-a9f4-0fba31166898";

        // TODO
        return null;
    }

    private ClassificationDef  getConfidentialityClassification()
    {
        final String guid = "742ddb7d-9a4a-4eb5-8ac2-1d69953bd2b6";

        // TODO
        return null;
    }

    private ClassificationDef  getConfidenceClassification()
    {
        final String guid = "25d8f8d5-2998-4983-b9ef-265f58732965";

        // TODO
        return null;
    }

    private ClassificationDef  getRetentionClassification()
    {
        final String guid = "83dbcdf2-9445-45d7-bb24-9fa661726553";

        // TODO
        return null;
    }

    private ClassificationDef  getCriticalityClassification()
    {
        final String guid = "d46d211a-bd22-40d5-b642-87b4954a167e";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0424 Governance Zones define the zones used to group assets according to their use.
     */
    void add0424GovernanceZones()
    {
        this.archiveBuilder.addEntityDef(getGovernanceZoneEntity());

        this.archiveBuilder.addRelationshipDef(getZoneGovernanceRelationship());
        this.archiveBuilder.addRelationshipDef(getZoneMembershipRelationship());
    }

    private EntityDef  getGovernanceZoneEntity()
    {
        final String guid = "290a192b-42a7-449a-935a-269ca62cfdac";

        // TODO
        return null;
    }

    private RelationshipDef  getZoneGovernanceRelationship()
    {
        final String guid = "4c4d1d9c-a9fc-4305-8b71-4e891c0f9ae0";

        // TODO
        return null;
    }

    private RelationshipDef  getZoneMembershipRelationship()
    {
        final String guid = "8866f770-3f8f-4062-ad2a-2841d4f7ceab";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0430 Technical Controls describe specific types of technical controls
     */
    void add0430TechnicalControls()
    {
        this.archiveBuilder.addEntityDef(getGovernanceRuleEntity());
        this.archiveBuilder.addEntityDef(getGovernanceProcessEntity());

        this.archiveBuilder.addClassificationDef(getControlPointClassification());
        this.archiveBuilder.addClassificationDef(getVerificationPointClassification());
        this.archiveBuilder.addClassificationDef(getEnforcementPointClassification());

    }

    private EntityDef  getGovernanceRuleEntity()
    {
        final String guid = "8f954380-12ce-4a2d-97c6-9ebe250fecf8";

        // TODO
        return null;
    }

    private EntityDef  getGovernanceProcessEntity()
    {
        final String guid = "b68b5d9d-6b79-4f3a-887f-ec0f81c54aea";

        // TODO
        return null;
    }

    private ClassificationDef  getControlPointClassification()
    {
        final String guid = "acf8b73e-3545-435d-ba16-fbfae060dd28";

        // TODO
        return null;
    }

    private ClassificationDef  getVerificationPointClassification()
    {
        final String guid = "12d78c95-3879-466d-883f-b71f6477a741";

        // TODO
        return null;
    }

    private ClassificationDef  getEnforcementPointClassification()
    {
        final String guid = "f4ce104e-7430-4c30-863d-60f6af6394d9";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0435 Governance Rules define details of a governance rule implementation.
     */
    void add0435GovernanceRules()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0438 Naming Standards provides definitions for laying our naming standards for schemas and assets.
     */
    void add0438NamingStandards()
    {
        this.archiveBuilder.addEntityDef(getNamingConventionRuleEntity());

        this.archiveBuilder.addRelationshipDef(getNamingStandardRuleSetRelationship());

        this.archiveBuilder.addClassificationDef(getNamingGuidanceClassification());
        this.archiveBuilder.addClassificationDef(getPrimeWordClassification());
        this.archiveBuilder.addClassificationDef(getClassWordClassification());
        this.archiveBuilder.addClassificationDef(getModifierClassification());
        this.archiveBuilder.addClassificationDef(getNamingStandardRuleSetClassification());
    }


    private EntityDef  getNamingConventionRuleEntity()
    {
        final String guid = "52505b06-98a5-481f-8a32-db9b02afabfc";

        // TODO
        return null;
    }

    private RelationshipDef  getNamingStandardRuleSetRelationship()
    {
        final String guid = "8a480b6a-b05e-45a5-af29-2313356f5104";

        // TODO
        return null;
    }

    private ClassificationDef  getNamingGuidanceClassification()
    {
        final String guid = "79ed6441-4bd4-44ef-98df-dc9fb516ab9a";

        // TODO
        return null;
    }

    private ClassificationDef  getPrimeWordClassification()
    {
        final String guid = "3ea1ea66-8923-4662-8628-0bacef3e9c5f";

        // TODO
        return null;
    }

    private ClassificationDef  getClassWordClassification()
    {
        final String guid = "feac4bd9-37d9-4437-82f6-618ce3e2793e";

        // TODO
        return null;
    }

    private ClassificationDef  getModifierClassification()
    {
        final String guid = "dfc70bed-7e8b-4060-910c-59c7473f23a3";

        // TODO
        return null;
    }

    private ClassificationDef  getNamingStandardRuleSetClassification()
    {
        final String guid = "ba70f506-1f81-4890-bb4f-1cb1d99c939e";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0440 Organizational Controls describe organizational roles and responsibilities.
     */
    void add0440OrganizationalControls()
    {
        this.archiveBuilder.addEnumDef(getBusinessCapabilityTypeEnum());

        this.archiveBuilder.addEntityDef(getBusinessCapabilityEntity());
        this.archiveBuilder.addEntityDef(getGovernanceResponsiblityEntity());
        this.archiveBuilder.addEntityDef(getGovernanceProcedureEntity());

        this.archiveBuilder.addRelationshipDef(getResponsibilityStaffContactRelationship());
        this.archiveBuilder.addRelationshipDef(getBusinessCapabilityResponsibilityRelationship());
    }

    private EnumDef  getBusinessCapabilityTypeEnum()
    {
        final String guid = "fb7c40cf-8d95-48ff-ba8b-e22bff6f5a91";

        // TODO
        return null;
    }

    private EntityDef  getBusinessCapabilityEntity()
    {
        final String guid = "7cc6bcb2-b573-4719-9412-cf6c3f4bbb15";

        // TODO
        return null;
    }

    private EntityDef  getGovernanceResponsiblityEntity()
    {
        final String guid = "89a76b24-deb8-45bf-9304-a578a610326f";

        // TODO
        return null;
    }

    private EntityDef  getGovernanceProcedureEntity()
    {
        final String guid = "69055d10-51dc-4c2b-b21f-d76fad3f8ef3";

        // TODO
        return null;
    }

    private RelationshipDef  getResponsibilityStaffContactRelationship()
    {
        final String guid = "47f0ad39-db77-41b0-b406-36b1598e0ba7";

        // TODO
        return null;
    }

    private RelationshipDef  getBusinessCapabilityResponsibilityRelationship()
    {
        final String guid = "b5de932a-738c-4c69-b852-09fec2b9c678";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0442ProjectCharter()
    {
        this.archiveBuilder.addEntityDef(getProjectCharterEntity());

        this.archiveBuilder.addRelationshipDef(getProjectCharterLinkRelationship());
    }

    private EntityDef  getProjectCharterEntity()
    {
        final String guid = "f96b5a32-42c1-4a74-8f77-70a81cec783d";

        // TODO
        return null;
    }

    private RelationshipDef  getProjectCharterLinkRelationship()
    {
        final String guid = "f081808d-545a-41cb-a9aa-c4f074a16c78";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0445GovernanceRoles()
    {
        this.archiveBuilder.addRelationshipDef(getStaffAssignmentRelationship());
    }

    private RelationshipDef  getStaffAssignmentRelationship()
    {
        final String guid = "cb10c107-b7af-475d-aab0-d78b8297b982";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0447GovernanceProcesses()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0450GovernanceRollout()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0452GovernanceDaemons()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0455ExceptionManagement()
    {
        this.archiveBuilder.addClassificationDef(getExceptionLogFileClassification());
        this.archiveBuilder.addClassificationDef(getAuditLogFileClassification());
        this.archiveBuilder.addClassificationDef(getExceptionBacklogClassification());
        this.archiveBuilder.addClassificationDef(getAuditLogClassification());
        this.archiveBuilder.addClassificationDef(getMeteringLogClassification());
        this.archiveBuilder.addClassificationDef(getStewardshipServerClassification());
    }

    private ClassificationDef  getExceptionLogFileClassification()
    {
        final String guid = "4756a6da-e0c2-4e81-b9ab-99df2f735eec";

        // TODO
        return null;
    }

    private ClassificationDef  getAuditLogFileClassification()
    {
        final String guid = "109d6d13-a3cf-4687-a0c1-c3802dc6b3a2";

        // TODO
        return null;
    }

    private ClassificationDef  getExceptionBacklogClassification()
    {
        final String guid = "b3eceea3-aa02-4d84-8f11-da4953e64b5f";

        // TODO
        return null;
    }

    private ClassificationDef  getAuditLogClassification()
    {
        final String guid = "449be034-6cc8-4f1b-859f-a8b9ff8ee7a1";

        // TODO
        return null;
    }

    private ClassificationDef  getMeteringLogClassification()
    {
        final String guid = "161b37c9-1d51-433b-94ce-5a760a198236";

        // TODO
        return null;
    }

    private ClassificationDef  getStewardshipServerClassification()
    {
        final String guid = "eaaeaa31-6f8b-4ed5-88fe-422ed3733158";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0457SecurityCapabilities()
    {
        this.archiveBuilder.addEntityDef(getSecurityCapabilityEntity());

        this.archiveBuilder.addClassificationDef(getPolicyAdministrationPointClassification());
        this.archiveBuilder.addClassificationDef(getPolicyDecisionPointClassification());
        this.archiveBuilder.addClassificationDef(getPolicyEnforcementPointClassification());
        this.archiveBuilder.addClassificationDef(getPolicyInformationPointClassification());
        this.archiveBuilder.addClassificationDef(getPolicyRetrievalPointClassification());
    }

    private EntityDef  getSecurityCapabilityEntity()
    {
        final String guid = "ff9ad4db-8ad7-44c8-bacf-263f1ded5367";

        // TODO
        return null;
    }

    private ClassificationDef  getPolicyAdministrationPointClassification()
    {
        final String guid = "9ada8e7b-823c-40f7-adf8-f164aabda77e";

        // TODO
        return null;
    }

    private ClassificationDef  getPolicyDecisionPointClassification()
    {
        final String guid = "e076fbb3-54f5-46b8-8f1e-a7cb7e792673";

        // TODO
        return null;
    }

    private ClassificationDef  getPolicyEnforcementPointClassification()
    {
        final String guid = "c2aa2738-eb96-4831-a63d-a7cd4193010a";

        // TODO
        return null;
    }

    private ClassificationDef  getPolicyInformationPointClassification()
    {
        final String guid = "1bc6fe1c-cfef-4fe9-a57b-0952359c7246";

        // TODO
        return null;
    }

    private ClassificationDef  getPolicyRetrievalPointClassification()
    {
        final String guid = "d8442cae-70f5-4888-a714-82a07c7f4721";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0460GovernanceActions()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0480RightsManagement()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0481Licenses()
    {
        this.archiveBuilder.addEntityDef(getLicenseTypeEntity());

        this.archiveBuilder.addRelationshipDef(getLicenseRelationship());
    }

    private EntityDef  getLicenseTypeEntity()
    {
        final String guid = "046a049d-5f80-4e5b-b0ae-f3cf6009b513";

        // TODO
        return null;
    }

    private RelationshipDef  getLicenseRelationship()
    {
        final String guid = "35e53b7f-2312-4d66-ae90-2d4cb47901ee";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0482Certifications()
    {
        this.archiveBuilder.addEntityDef(getCertificationTypeEntity());

        this.archiveBuilder.addRelationshipDef(getCertificationRelationship());
    }

    private EntityDef  getCertificationTypeEntity()
    {
        final String guid = "97f9ffc9-e2f7-4557-ac12-925257345eea";

        // TODO
        return null;
    }

    private RelationshipDef  getCertificationRelationship()
    {
        final String guid = "390559eb-6a0c-4dd7-bc95-b9074caffa7f";

        // TODO
        return null;
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
    void add0501SchemaElements()
    {
        this.archiveBuilder.addEntityDef(getSchemaElementEntity());
        this.archiveBuilder.addEntityDef(getPrimitiveSchemaElementEntity());
    }

    private EntityDef  getSchemaElementEntity()
    {
        final String guid = "718d4244-8559-49ed-ad5a-10e5c305a656";

        // TODO
        return null;
    }

    private EntityDef  getPrimitiveSchemaElementEntity()
    {
        final String guid = "f0f75fba-9136-4082-8352-0ad74f3c36ed";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0503 Asset Schemas shows how assets are linked to schemas
     */
    void add0503AssetSchemas()
    {
        this.archiveBuilder.addRelationshipDef(getAssetSchemaRelationship());
    }

    private RelationshipDef  getAssetSchemaRelationship()
    {
        final String guid = "815b004d-73c6-4728-9dd9-536f4fe803cd";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0504 Implementation snippets enable code fragments defining data structures to be linked with the
     * relevant schema to show how the schema should be implemented.
     */
    void add0504ImplementationSnippets()
    {
        this.archiveBuilder.addEntityDef(getImplementationSnippetEntity());

        this.archiveBuilder.addRelationshipDef(getSchemaElementImplementationRelationship());
    }

    private EntityDef  getImplementationSnippetEntity()
    {
        final String guid = "49990755-2faa-4a62-a1f3-9124b9c73df4";

        // TODO
        return null;
    }

    private RelationshipDef  getSchemaElementImplementationRelationship()
    {
        final String guid = "6aab4ec6-f0c6-4c40-9f50-ac02a3483358";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0505 Schema Attributes begins to fill out the structure of a schema.
     */
    void add0505SchemaAttributes()
    {
        this.archiveBuilder.addEntityDef(getSchemaAttributeEntity());
        this.archiveBuilder.addEntityDef(getSchemaEntity());
        this.archiveBuilder.addEntityDef(getStructSchemaEntity());
        this.archiveBuilder.addEntityDef(getBoundedSchemaEntity());
        this.archiveBuilder.addEntityDef(getArraySchemaEntity());
        this.archiveBuilder.addEntityDef(getSetSchemaEntity());

        this.archiveBuilder.addRelationshipDef(getSchemaAttributesRelationship());
        this.archiveBuilder.addRelationshipDef(getSchemaAttributeTypeRelationship());

        this.archiveBuilder.addClassificationDef(getPrimaryKeyClassification());
        this.archiveBuilder.addClassificationDef(getForeignKeyClassification());
    }

    private EntityDef  getSchemaAttributeEntity()
    {
        final String guid = "1a5e159b-913a-43b1-95fe-04433b25fca9";

        // TODO
        return null;
    }

    private EntityDef  getSchemaEntity()
    {
        final String guid = "786a6199-0ce8-47bf-b006-9ace1c5510e4";

        // TODO
        return null;
    }

    private EntityDef  getStructSchemaEntity()
    {
        final String guid = "a13b409f-fd67-4506-8d94-14dfafd250a4";

        // TODO
        return null;
    }

    private EntityDef  getBoundedSchemaEntity()
    {
        final String guid = "77133161-37a9-43f5-aaa3-fd6d7ff92fdb";

        // TODO
        return null;
    }

    private EntityDef  getArraySchemaEntity()
    {
        final String guid = "ba8d29d2-a8a4-41f3-b29f-91ad924dd944";

        // TODO
        return null;
    }

    private EntityDef  getSetSchemaEntity()
    {
        final String guid = "b2605d2d-10cd-443c-b3e8-abf15fb051f0";

        // TODO
        return null;
    }

    private RelationshipDef  getSchemaAttributesRelationship()
    {
        final String guid = "86b176a2-015c-44a6-8106-54d5d69ba661";

        // TODO
        return null;
    }

    private RelationshipDef  getSchemaAttributeTypeRelationship()
    {
        final String guid = "2d955049-e59b-45dd-8e62-cde1add59f9e";

        // TODO
        return null;
    }

    private ClassificationDef  getPrimaryKeyClassification()
    {
        final String guid = "b239d832-50bd-471b-b17a-15a335fc7f40";

        // TODO
        return null;
    }

    private ClassificationDef  getForeignKeyClassification()
    {
        final String guid = "3cd4e0e7-fdbf-47a6-ae88-d4b3205e0c07";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0510 Schema Link Elements defines how a link between two schema elements in different schema element
     * hierarchies are linked.
     */
    void add0510SchemaLinkElements()
    {
        this.archiveBuilder.addEntityDef(getSchemaLinkElementEntity());

        this.archiveBuilder.addRelationshipDef(getLinkedAttributeRelationship());
        this.archiveBuilder.addRelationshipDef(getSchemaLinkAttributeRelationship());
    }

    private EntityDef  getSchemaLinkElementEntity()
    {
        final String guid = "67e08705-2d2a-4df6-9239-1818161a41e0";

        // TODO
        return null;
    }

    private RelationshipDef  getLinkedAttributeRelationship()
    {
        final String guid = "292125f7-5660-4533-a48a-478c5611922e";

        // TODO
        return null;
    }

    private RelationshipDef  getSchemaLinkAttributeRelationship()
    {
        final String guid = "db9583c5-4690-41e5-a580-b4e30a0242d3";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0511 Schema Map Elements defines how a map element is recorded in a schema.
     */
    void add0511SchemaMapElements()
    {
        this.archiveBuilder.addEntityDef(getMapSchemaEntity());

        this.archiveBuilder.addRelationshipDef(getMapFromElementTypeRelationship());
        this.archiveBuilder.addRelationshipDef(getMapToElementTypeRelationship());
    }

    private EntityDef  getMapSchemaEntity()
    {
        final String guid = "bd4c85d0-d471-4cd2-a193-33b0387a19fd";

        // TODO
        return null;
    }

    private RelationshipDef  getMapFromElementTypeRelationship()
    {
        final String guid = "6189d444-2da4-4cd7-9332-e48a1c340b44";

        // TODO
        return null;
    }

    private RelationshipDef  getMapToElementTypeRelationship()
    {
        final String guid = "8b9856b3-451e-45fc-afc7-fddefd81a73a";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */


    /**
     * 0512 Derived Schema Attributes defines how one attribute can be derived from another.
     */
    void add0512DerivedSchemaAttributes()
    {
        this.archiveBuilder.addEntityDef(getDerivedSchemaAttributeEntity());

        this.archiveBuilder.addRelationshipDef(getSchemaQueryImplementationRelationship());
    }

    private EntityDef  getDerivedSchemaAttributeEntity()
    {
        final String guid = "cf21abfe-655a-47ba-b9b6-f73394745c80";

        // TODO
        return null;
    }

    private RelationshipDef  getSchemaQueryImplementationRelationship()
    {
        final String guid = "e5d7025d-8b4f-43c7-bcae-1047d650b94a";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0530 describes table oriented schemas such as spreadsheets
     */
    void add0530TabularSchemas()
    {
        this.archiveBuilder.addEntityDef(getTabularSchemaEntity());
        this.archiveBuilder.addEntityDef(getDerivedTabularColumnTypeEntity());
        this.archiveBuilder.addEntityDef(getTabularColumnTypeEntity());
        this.archiveBuilder.addEntityDef(getTabularColumnEntity());
    }

    private EntityDef  getTabularSchemaEntity()
    {
        final String guid = "248975ec-8019-4b8a-9caf-084c8b724233";

        // TODO
        return null;
    }

    private EntityDef  getDerivedTabularColumnTypeEntity()
    {
        final String guid = "65b3c5cf-5f20-49fb-b023-bb3680131127";

        // TODO
        return null;
    }

    private EntityDef  getTabularColumnTypeEntity()
    {
        final String guid = "a7392281-348d-48a4-bad7-f9742d7696fe";

        // TODO
        return null;
    }

    private EntityDef  getTabularColumnEntity()
    {
        final String guid = "d81a0425-4e9b-4f31-bc1c-e18c3566da10";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0531 Document Schema provide specialized entities for describing document oriented schemas such as JSON
     */
    void add0531DocumentSchemas()
    {
        this.archiveBuilder.addEntityDef(getDocumentSchemaEntity());
        this.archiveBuilder.addEntityDef(getSimpleDocumentElementEntity());
        this.archiveBuilder.addEntityDef(getStructDocumentElementEntity());
        this.archiveBuilder.addEntityDef(getArrayDocumentElementEntity());
        this.archiveBuilder.addEntityDef(getSetDocumentElementEntity());
        this.archiveBuilder.addEntityDef(getMapDocumentElementEntity());
    }

    private EntityDef  getDocumentSchemaEntity()
    {
        final String guid = "33da99cd-8d04-490c-9457-c58908da7794";

        // TODO
        return null;
    }

    private EntityDef  getSimpleDocumentElementEntity()
    {
        final String guid = "42cfccbf-cc68-4980-8c31-0faf1ee002d3";

        // TODO
        return null;
    }

    private EntityDef  getStructDocumentElementEntity()
    {
        final String guid = "f6245c25-8f73-45eb-8fb5-fa17a5f27649";

        // TODO
        return null;
    }

    private EntityDef  getArrayDocumentElementEntity()
    {
        final String guid = "ddd29c67-db9a-45ff-92aa-6d17a12a8ee2";

        // TODO
        return null;
    }

    private EntityDef  getSetDocumentElementEntity()
    {
        final String guid = "67228a7a-9d8d-4fa7-b217-17474f1f4ac6";

        // TODO
        return null;
    }

    private EntityDef  getMapDocumentElementEntity()
    {
        final String guid = "b0f09598-ceb6-415b-befc-563ecadd5727";

        // TODO
        return null;
    }


    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0532 Object Schemas provides the specialized entity for an object schema.
     */
    void add0532ObjectSchemas()
    {
        this.archiveBuilder.addEntityDef(getObjectSchemaEntity());
    }

    private EntityDef  getObjectSchemaEntity()
    {
        final String guid = "6920fda1-7c07-47c7-84f1-9fb044ae153e";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0533GraphSchemas()
    {
        this.archiveBuilder.addEntityDef(getGraphSchemaEntity());
        this.archiveBuilder.addEntityDef(getGraphVertexTypeEntity());
        this.archiveBuilder.addEntityDef(getGraphEdgeTypeEntity());
    }

    private EntityDef  getGraphSchemaEntity()
    {
        final String guid = "983c5e72-801b-4e42-bc51-f109527f2317";

        // TODO
        return null;
    }

    private EntityDef  getGraphVertexTypeEntity()
    {
        final String guid = "1252ce12-540c-4724-ad70-f70940956de0";

        // TODO
        return null;
    }

    private EntityDef  getGraphEdgeTypeEntity()
    {
        final String guid = "d4104eb3-4f2d-4d83-aca7-e58dd8d5e0b1";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0534 Relational Schemas extend the tabular schemas to describe a relational database.
     */
    void add0534RelationalSchemas()
    {
        this.archiveBuilder.addEntityDef(getRationalSchemaEntity());
        this.archiveBuilder.addEntityDef(getRelationalTableEntity());
        this.archiveBuilder.addEntityDef(getRelationalViewEntity());
        this.archiveBuilder.addEntityDef(getRelationalColumnEntity());
        this.archiveBuilder.addEntityDef(getRelationalColumnTypeEntity());
        this.archiveBuilder.addEntityDef(getDerivedRelationalColumnEntity());
    }

    private EntityDef  getRationalSchemaEntity()
    {
        final String guid = "f20f5f45-1afb-41c1-9a09-34d8812626a4";

        // TODO
        return null;
    }

    private EntityDef  getRelationalTableEntity()
    {
        final String guid = "ce7e72b8-396a-4013-8688-f9d973067425";

        // TODO
        return null;
    }

    private EntityDef  getRelationalViewEntity()
    {
        final String guid = "4814bec8-482d-463d-8376-160b0358e129";

        // TODO
        return null;
    }

    private EntityDef  getRelationalColumnEntity()
    {
        final String guid = "aa8d5470-6dbc-4648-9e2f-045e5df9d2f9";

        // TODO
        return null;
    }

    private EntityDef  getRelationalColumnTypeEntity()
    {
        final String guid = "f0438d80-6eb9-4fac-bcc1-5efee5babcfc";

        // TODO
        return null;
    }

    private EntityDef  getDerivedRelationalColumnEntity()
    {
        final String guid = "a9f7d15d-b797-450a-8d56-1ba55490c019";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0535 Event Schemas describes event/message structures
     */
    void add0535EventSchemas()
    {
        this.archiveBuilder.addEntityDef(getEventSetEntity());
        this.archiveBuilder.addEntityDef(getEventTypeEntity());
    }

    private EntityDef  getEventSetEntity()
    {
        final String guid = "bead9aa4-214a-4596-8036-aa78395bbfb1";

        // TODO
        return null;
    }

    private EntityDef  getEventTypeEntity()
    {
        final String guid = "8bc88aba-d7e4-4334-957f-cfe8e8eadc32";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 0536 API schemas define the structure of an API
     */
    void add0536APISchemas()
    {
        this.archiveBuilder.addEntityDef(getAPISchemaEntity());
        this.archiveBuilder.addEntityDef(getAPIOperationSchemaEntity());

        this.archiveBuilder.addRelationshipDef(getAPIOperationsRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIHeaderRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIRequestRelationship());
        this.archiveBuilder.addRelationshipDef(getAPIResponseRelationship());

        this.archiveBuilder.addClassificationDef(getAPIOperationClassification());
    }

    private EntityDef  getAPISchemaEntity()
    {
        final String guid = "b46cddb3-9864-4c5d-8a49-266b3fc95cb8";

        // TODO
        return null;
    }

    private EntityDef  getAPIOperationSchemaEntity()
    {
        final String guid = "f1c0af19-2729-4fac-996e-a7badff3c21c";

        // TODO
        return null;
    }

    private RelationshipDef  getAPIOperationsRelationship()
    {
        final String guid = "03737169-ceb5-45f0-84f0-21c5929945af";

        // TODO
        return null;
    }

    private RelationshipDef  getAPIHeaderRelationship()
    {
        final String guid = "e8fb46d1-5f75-481b-aa66-f43ad44e2cc6";

        // TODO
        return null;
    }

    private RelationshipDef  getAPIRequestRelationship()
    {
        final String guid = "4ab3b466-31bd-48ea-8aa2-75623476f2e2";

        // TODO
        return null;
    }

    private RelationshipDef  getAPIResponseRelationship()
    {
        final String guid = "e8001de2-1bb1-442b-a66f-9addc3641eae";

        // TODO
        return null;
    }

    private ClassificationDef  getAPIOperationClassification()
    {
        final String guid = "665391d3-6c3b-4c8e-af87-40f3bd9c4e72";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0550LogicSpecificationModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0560MappingModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0565ModelElements()
    {
        this.archiveBuilder.addEntityDef(getModelElementEntity());

        this.archiveBuilder.addRelationshipDef(getModelImplementationRelationship());
    }

    private EntityDef  getModelElementEntity()
    {
        final String guid = "492e343f-2516-43b8-94b0-5bae0760dda6";

        // TODO
        return null;
    }

    private RelationshipDef  getModelImplementationRelationship()
    {
        final String guid = "4ff6d91b-3836-4ba2-9ca9-87da91081faa";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0570MetaModel()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void add0575ProcessSchemas()
    {
        /* placeholder */
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    /**
     * 580 Solution Blueprints enable the recording of solution component models
     */
    void add580SolutionBlueprints()
    {
        this.archiveBuilder.addEntityDef(getSolutionBlueprintEntity());
        this.archiveBuilder.addEntityDef(getSolutionBlueprintTemplateEntity());
        this.archiveBuilder.addEntityDef(getNestedSolutionBlueprintEntity());

        this.archiveBuilder.addRelationshipDef(getSolutionTypeRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionBlueprintComponentRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionBlueprintHierarchyRelationship());
    }

    private EntityDef  getSolutionBlueprintEntity()
    {
        final String guid = "4aa47799-5128-4eeb-bd72-e357b49f8bfe";

        // TODO
        return null;
    }

    private EntityDef  getSolutionBlueprintTemplateEntity()
    {
        final String guid = "f671e1fc-b204-4ee6-a4e2-da1633ecf50e";

        // TODO
        return null;
    }

    private EntityDef  getNestedSolutionBlueprintEntity()
    {
        final String guid = "b83f3d42-f3f7-4155-ae65-58fb44ea7644";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionTypeRelationship()
    {
        final String guid = "f1ae975f-f11a-467b-8c7a-b023081e4712";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionBlueprintComponentRelationship()
    {
        final String guid = "a43b4c9c-52c2-4819-b3cc-9d07d49a11f2";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionBlueprintHierarchyRelationship()
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
    void add0581SolutionPortsAndWires()
    {
        this.archiveBuilder.addEntityDef(getPortEntity());

        this.archiveBuilder.addRelationshipDef(getSolutionLinkingWireRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionPortRelationship());
        this.archiveBuilder.addRelationshipDef(getSolutionPortDelgationRelationship());
    }

    private EntityDef  getPortEntity()
    {
        final String guid = "62ef448c-d4c1-4c94-a565-5e5625f6a57b";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionLinkingWireRelationship()
    {
        final String guid = "892a3d1c-cfb8-431d-bd59-c4d38833bfb0";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionPortRelationship()
    {
        final String guid = "5652d03a-f6c9-411a-a3e4-f490d3856b64";

        // TODO
        return null;
    }

    private RelationshipDef  getSolutionPortDelgationRelationship()
    {
        final String guid = "8335e6ed-fd86-4000-9bc5-5203062f28ba";

        // TODO
        return null;
    }

    /*
     * -------------------------------------------------------------------------------------------------------
     */

    void addReferenceData()
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
    void add0601MetadataDiscoveryServer()
    {
        this.archiveBuilder.addEntityDef(getDiscoveryServiceEntity());

        this.archiveBuilder.addClassificationDef(getMetadataDiscoveryServerClassification());
    }

    private EntityDef  getDiscoveryServiceEntity()
    {
        final String guid = "2f278dfc-4640-4714-b34b-303e84e4fc40";

        // TODO
        return null;
    }

    private ClassificationDef  getMetadataDiscoveryServerClassification()
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
    void add0605DiscoveryAnalysisReports()
    {
        this.archiveBuilder.addEntityDef(getDiscoveryAnalysisReportEntity());

        this.archiveBuilder.addRelationshipDef(getDiscoveryServerReportRelationship());
        this.archiveBuilder.addRelationshipDef(getDiscoveryServiceReportRelationship());
    }


    private EntityDef  getDiscoveryAnalysisReportEntity()
    {
        final String guid = "acc7cbc8-09c3-472b-87dd-f78459323dcb";

        // TODO
        return null;
    }

    private RelationshipDef  getDiscoveryServerReportRelationship()
    {
        final String guid = "2c318c3a-5dc2-42cd-a933-0087d852f67f";

        // TODO
        return null;
    }

    private RelationshipDef  getDiscoveryServiceReportRelationship()
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
    void add0610Annotations()
    {
        this.archiveBuilder.addEnumDef(getAnnotationStatusEnum());

        this.archiveBuilder.addEntityDef(getAnnotationEntity());
        this.archiveBuilder.addEntityDef(getAnnotationReviewEntity());

        this.archiveBuilder.addRelationshipDef(getDiscoveredAnnotationRelationship());
        this.archiveBuilder.addRelationshipDef(getAssetAnnotationAttachmentRelationship());
        this.archiveBuilder.addRelationshipDef(getAnnotationReviewLinkRelationship());
    }

    private EnumDef  getAnnotationStatusEnum()
    {
        final String guid = "71187df6-ef66-4f88-bc03-cd3c7f925165";

        // TODO
        return null;
    }

    private EntityDef  getAnnotationEntity()
    {
        final String guid = "6cea5b53-558c-48f1-8191-11d48db29fb4";

        // TODO
        return null;
    }

    private EntityDef  getAnnotationReviewEntity()
    {
        final String guid = "b893d6fc-642a-454b-beaf-809ee4dd876a";

        // TODO
        return null;
    }

    private RelationshipDef  getDiscoveredAnnotationRelationship()
    {
        final String guid = "51d386a3-3857-42e3-a3df-14a6cad08b93";

        // TODO
        return null;
    }

    private RelationshipDef  getAssetAnnotationAttachmentRelationship()
    {
        final String guid = "6056806d-682e-405c-964b-ca6fdc94be1b";

        // TODO
        return null;
    }

    private RelationshipDef  getAnnotationReviewLinkRelationship()
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
    void add0615SchemaExtraction()
    {
        this.archiveBuilder.addEntityDef(getSchemaAnnotationEntity());

        this.archiveBuilder.addRelationshipDef(getNestedSchemaStructureRelationship());
    }

    private EntityDef  getSchemaAnnotationEntity()
    {
        final String guid = "3c5aa68b-d562-4b04-b189-c7b7f0bf2ced";

        // TODO
        return null;
    }

    private RelationshipDef  getNestedSchemaStructureRelationship()
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
    void add0620Profiling()
    {
        this.archiveBuilder.addEntityDef(getProfilingAnnotationEntity());
    }

    private EntityDef  getProfilingAnnotationEntity()
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
    void add0625SemanticDiscovery()
    {
        this.archiveBuilder.addEntityDef(getSemanticAnnotationEntity());
    }

    private EntityDef  getSemanticAnnotationEntity()
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
    void add0630RelationshipDiscovery()
    {
        this.archiveBuilder.addRelationshipDef(getRelationshipAnnotationRelationship());
    }

    private RelationshipDef  getRelationshipAnnotationRelationship()
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
    void add0635ClassificationDiscovery()
    {
        this.archiveBuilder.addClassificationDef(getClassificationAnnotationClassification());
    }


    private ClassificationDef  getClassificationAnnotationClassification()
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
    void add0650Measurements()
    {
        this.archiveBuilder.addEntityDef(getDataSetMeasurementAnnotationEntity());
        this.archiveBuilder.addEntityDef(getDataSetPhysicalStatusAnnotationEntity());
    }

    private EntityDef  getDataSetMeasurementAnnotationEntity()
    {
        final String guid = "c85bea73-d7af-46d7-8a7e-cb745910b1df";

        // TODO
        return null;
    }

    private EntityDef  getDataSetPhysicalStatusAnnotationEntity()
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
    void add0660RequestForAction()
    {
        this.archiveBuilder.addEntityDef(getRequestForActionAnnotationEntity());
    }

    private EntityDef  getRequestForActionAnnotationEntity()
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
