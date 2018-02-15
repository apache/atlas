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

import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.*;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSRepositoryConnector;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceStatus;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * OMRSRepositoryContentManager supports an in-memory cache of TypeDefs for the local server.  It is used by the OMRS
 * components for constructing metadata instances with valid types.   It ensures that the TypeDefs used in other
 * members of the open metadata repository cohorts that the local server is also a member of are consistent with the
 * local repository.
 *
 * OMRSRepositoryContentManager plays a central role in ensuring the integrity of the metadata in the local repository.
 * It is called from multiple components at different points in the processing.  It presents a different interface
 * to each of these components that is specialized to their needs.
 * <ul>
 *     <li>
 *         OMRSTypeDefEventProcessor - processes inbound events from remote members of the open metadata
 *         repository cohorts that the local repository is connected to.  These incoming TypeDef events need to
 *         be validated against the types used locally and either saved or discarded depending on the exchange rule
 *         setting.
 *     </li>
 *     <li>
 *         OMRSTypeDefManager - provides maintenance methods for managing the TypeDefs in the local cache.
 *     </li>
 *     <li>
 *         OMRSTypeDefHelper - provides methods to help OMRS connectors and adapters manage TypeDefs.
 *     </li>
 *     <li>
 *         OMRSTypeDefValidator - provides methods to validate TypeDefs.
 *     </li>
 *     <li>
 *         OMRSInstanceValidator - provides methods to help validate instances.
 *     </li>
 * </ul>
 */
public class OMRSRepositoryContentManager implements OMRSTypeDefEventProcessor,
                                                     OMRSTypeDefManager,
                                                     OMRSTypeDefHelper,
                                                     OMRSTypeDefValidator,
                                                     OMRSInstanceValidator
{
    private String                            localMetadataCollectionId      = null;
    private LocalOMRSRepositoryConnector      localRepositoryConnector       = null;
    private OMRSRepositoryEventManager        outboundRepositoryEventManager = null;
    private OMRSRepositoryConnector           realLocalConnector             = null;
    private OMRSRepositoryEventExchangeRule   saveExchangeRule               = null;
    private String                            openTypesOriginGUID            = null;
    private HashMap<String, TypeDef>          knownTypes                     = new HashMap<>();
    private HashMap<String, AttributeTypeDef> knownAttributeTypes            = new HashMap<>();
    private HashMap<String, TypeDef>          activeTypes                    = null;
    private HashMap<String, AttributeTypeDef> activeAttributeTypes           = null;



    /*
     * The audit log provides a verifiable record of the open metadata archives that have been loaded into
     * the open metadata repository.  The Logger is for standard debug.
     */
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.TYPEDEF_MANAGER);
    private static final Logger       log      = LoggerFactory.getLogger(OMRSRepositoryContentManager.class);



    /**
     * Default constructor
     */
    public OMRSRepositoryContentManager()
    {

    }


    /**
     * Saves all of the information necessary to process incoming TypeDef events.
     *
     * @param localRepositoryConnector - connector to the local repository
     * @param realLocalConnector - connector to the real local repository - used for processing TypeDef events
     * @param saveExchangeRule - rule that determines which events to process.
     * @param outboundRepositoryEventManager - event manager to call for outbound events - used to send out reports
     *                                       of conflicting TypeDefs
     */
    public void setupEventProcessor(LocalOMRSRepositoryConnector      localRepositoryConnector,
                                    OMRSRepositoryConnector           realLocalConnector,
                                    OMRSRepositoryEventExchangeRule   saveExchangeRule,
                                    OMRSRepositoryEventManager        outboundRepositoryEventManager)
    {
        this.localRepositoryConnector       = localRepositoryConnector;
        this.realLocalConnector             = realLocalConnector;
        this.saveExchangeRule               = saveExchangeRule;
        this.localMetadataCollectionId      = localRepositoryConnector.getMetadataCollectionId();
        this.outboundRepositoryEventManager = outboundRepositoryEventManager;
    }


    /**
     * Save the unique identifier of the open metadata archive.  This is stored in the origin property of
     * all of the open metadata types.  It is needed to support the isOpenType() method.
     *
     * @param openMetadataTypesGUID - unique identifier for the open metadata type's archive
     */
    public void setOpenMetadataTypesOriginGUID(String openMetadataTypesGUID)
    {
        openTypesOriginGUID = openMetadataTypesGUID;
    }


    /*
     * ========================
     * OMRSTypeDefManager
     */

    /**
     * Cache a definition of a new TypeDef.  This method assumes the TypeDef has been successfully added to the
     * local repository already and all that is needed is to maintain the cached list of types
     *
     * @param sourceName - source of the request (used for logging)
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     */
    public void addTypeDef(String  sourceName, TypeDef      newTypeDef)
    {
        if (this.validTypeDef(sourceName, newTypeDef))
        {
            knownTypes.put(newTypeDef.getName(), newTypeDef);
            if (localRepositoryConnector != null)
            {
                activeTypes.put(newTypeDef.getName(), newTypeDef);

                if (log.isDebugEnabled())
                {
                    log.debug("New Active Type " + newTypeDef.getName() + " from " + sourceName, newTypeDef);
                }

                // TODO log new active type
            }
        }
    }


    /**
     * Cache a definition of a new AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param newAttributeTypeDef - AttributeTypeDef structure describing the new TypeDef.
     */
    public void addAttributeTypeDef(String  sourceName, AttributeTypeDef newAttributeTypeDef)
    {

    }

    /**
     * Update one or more properties of a cached TypeDef.  This method assumes the TypeDef has been successfully
     * updated in the local repository already and all that is needed is to maintain the cached list of types
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDef - TypeDef structure.
     */
    public void updateTypeDef(String  sourceName, TypeDef   typeDef)
    {
        if (this.validTypeDef(sourceName, typeDef))
        {
            knownTypes.put(typeDef.getName(), typeDef);
            if (localRepositoryConnector != null)
            {
                activeTypes.put(typeDef.getName(), typeDef);

                if (log.isDebugEnabled())
                {
                    log.debug("Updated Active Type " + typeDef.getName() + " from " + sourceName, typeDef);
                }
            }
        }
    }


    /**
     * Delete a cached TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     */
    public void deleteTypeDef(String    sourceName,
                              String    obsoleteTypeDefGUID,
                              String    obsoleteTypeDefName)
    {
        if (this.validTypeId(sourceName, obsoleteTypeDefGUID, obsoleteTypeDefName))
        {
            knownTypes.remove(obsoleteTypeDefName);

            if (localRepositoryConnector != null)
            {
                activeTypes.remove(obsoleteTypeDefName);

                if (log.isDebugEnabled())
                {
                    log.debug("Deleted Active Type " + obsoleteTypeDefName + " from " + sourceName);
                }
            }
        }
    }


    /**
     * Delete a cached AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     */
    public void deleteAttributeTypeDef(String    sourceName,
                                       String    obsoleteTypeDefGUID,
                                       String    obsoleteTypeDefName)
    {

    }


    /**
     * Change the identifiers for a TypeDef.
     *
     * @param sourceName - source of the request (used for logging).
     * @param originalTypeDefGUID - TypeDef's original unique identifier.
     * @param originalTypeDefName - TypeDef's original unique name.
     * @param newTypeDef - updated TypeDef with new identifiers.
     */
    public void reIdentifyTypeDef(String   sourceName,
                                  String   originalTypeDefGUID,
                                  String   originalTypeDefName,
                                  TypeDef  newTypeDef)
    {
        // TODO
    }


    /**
     * Change the identifiers for an AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging).
     * @param originalAttributeTypeDefGUID - AttributeTypeDef's original unique identifier.
     * @param originalAttributeTypeDefName - AttributeTypeDef's original unique name.
     * @param newAttributeTypeDef - updated AttributeTypeDef with new identifiers
     */
    public void reIdentifyAttributeTypeDef(String            sourceName,
                                           String            originalAttributeTypeDefGUID,
                                           String            originalAttributeTypeDefName,
                                           AttributeTypeDef  newAttributeTypeDef)
    {
        // TODO
    }


    /**
     * Return the list of property names defined for this TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDef - type definition to work with.
     * @return list of String property names
     * @throws TypeErrorException - there is an issue with the TypeDef.
     */
    private ArrayList<String>  getPropertyNames(String sourceName, TypeDef   typeDef) throws TypeErrorException
    {
        final  String                  methodName = "getPropertyNames()";
        ArrayList<String>              propertyNames = null;

        if (validTypeDef(sourceName, typeDef))
        {
            ArrayList<TypeDefAttribute>    propertiesDefinition = typeDef.getPropertiesDefinition();

            if ((propertiesDefinition != null) && (propertiesDefinition.size() > 0))
            {
                propertyNames = new ArrayList<>();

                for (TypeDefAttribute  propertyDefinition : propertiesDefinition)
                {
                    if (propertyDefinition != null)
                    {
                        String propertyName = propertyDefinition.getAttributeName();

                        if (propertyName != null)
                        {
                            if (log.isDebugEnabled())
                            {
                                log.debug(typeDef.getName()  + " from " + sourceName + " has property " + propertyName);
                            }
                            propertyNames.add(propertyName);
                        }
                        else
                        {
                            OMRSErrorCode errorCode = OMRSErrorCode.BAD_TYPEDEF_ATTRIBUTE_NAME;
                            String errorMessage = errorCode.getErrorMessageId()
                                                + errorCode.getFormattedErrorMessage(sourceName);

                            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                                         this.getClass().getName(),
                                                         methodName,
                                                         errorMessage,
                                                         errorCode.getSystemAction(),
                                                         errorCode.getUserAction());
                        }
                    }
                    else
                    {
                        OMRSErrorCode errorCode = OMRSErrorCode.NULL_TYPEDEF_ATTRIBUTE;
                        String errorMessage = errorCode.getErrorMessageId()
                                            + errorCode.getFormattedErrorMessage(sourceName);

                        throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                                     this.getClass().getName(),
                                                     methodName,
                                                     errorMessage,
                                                     errorCode.getSystemAction(),
                                                     errorCode.getUserAction());
                    }
                }

                /*
                 * If no property names have been extracted then remove the array.
                 */
                if (propertyNames.size() == 0)
                {
                    propertyNames = null;
                }
            }
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.BAD_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(sourceName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
        }

        return propertyNames;
    }


    /**
     * Return identifiers for the TypeDef that matches the supplied type name.  If the type name is not recognized,
     * null is returned.
     *
     * @param sourceName - source of the request (used for logging)
     * @param category - category of the instance type required.
     * @param typeName - String type name.
     * @return InstanceType object containing TypeDef properties such as unique identifier (guid),
     *                             typeDef name and version name
     * @throws TypeErrorException - the type name is not a recognized type or is of the wrong category or there is
     *                              a problem with the cached TypeDef.
     */
    public InstanceType getInstanceType(String           sourceName,
                                        TypeDefCategory  category,
                                        String           typeName) throws TypeErrorException
    {
        final  String                  methodName = "getInstanceType()";

        if (isValidTypeCategory(sourceName, category, typeName))
        {
            TypeDef typeDef = knownTypes.get(typeName);

            if (typeDef != null)
            {
                InstanceType    instanceType = new InstanceType();

                instanceType.setTypeDefCategory(category);
                instanceType.setTypeDefGUID(typeDef.getGUID());
                instanceType.setTypeDefName(typeDef.getName());
                instanceType.setTypeDefVersion(typeDef.getVersion());
                instanceType.setTypeDefDescription(typeDef.getDescription());
                instanceType.setTypeDefDescriptionGUID(typeDef.getDescriptionGUID());

                /*
                 * Extract the properties for this TypeDef.  These will be augmented with property names
                 * from the super type(s).
                 */
                ArrayList<String>      propertyNames = this.getPropertyNames(sourceName, typeDef);

                /*
                 * If propertyNames is null, it means the TypeDef has no attributes.  However the superType
                 * may have attributes and so we need an array to accumulate the attributes into.
                 */
                if (propertyNames == null)
                {
                    propertyNames = new ArrayList<>();
                }

                /*
                 * Work up the TypeDef hierarchy extracting the property names and super type names.
                 */
                ArrayList<TypeDefLink> superTypes    = new ArrayList<>();
                TypeDefLink            superTypeLink = typeDef.getSuperType();

                while (superTypeLink != null)
                {
                    String                 superTypeName = superTypeLink.getName();

                    if (superTypeName != null)
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug(typeName + " from " + sourceName + " has super type " + superTypeName);
                        }

                        superTypes.add(superTypeLink);

                        TypeDef                superTypeDef  = knownTypes.get(superTypeName);

                        if (superTypeDef != null)
                        {
                            ArrayList<String>      superTypePropertyNames = this.getPropertyNames(sourceName, superTypeDef);

                            if (superTypePropertyNames != null)
                            {
                                propertyNames.addAll(0, superTypePropertyNames);
                            }
                        }

                        superTypeLink = superTypeDef.getSuperType();
                    }
                    else
                    {
                        superTypeLink = null;
                    }
                }

                /*
                 * Make sure empty lists are converted to nulls
                 */

                if (superTypes.size() > 0)
                {
                    instanceType.setTypeDefSuperTypes(superTypes);
                }

                if (propertyNames.size() > 0)
                {
                    instanceType.setValidInstanceProperties(propertyNames);
                }
            }
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.BAD_CATEGORY_FOR_TYPEDEF_ATTRIBUTE;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(sourceName, typeName, category.getTypeName());

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
        }

        return null;
    }


    /**
     * Return a boolean indicating that the type name matches the category.
     *
     * @param sourceName - source of the request (used for logging)
     * @param category - TypeDefCategory enum value to test
     * @param typeName - type name to test
     * @return - boolean flag indicating that the type name is of the specified category
     * @throws TypeErrorException - the type name is not a recognized type or there is
     *                              a problem with the cached TypeDef.
     */
    public boolean    isValidTypeCategory(String            sourceName,
                                          TypeDefCategory   category,
                                          String            typeName) throws TypeErrorException
    {
        if (category == null)
        {
            // TODO throw logic error
        }

        if (typeName == null)
        {
            // TODO throw logic error
        }

        TypeDef   typeDef = knownTypes.get(typeName);

        if (typeDef != null)
        {
            TypeDefCategory  retrievedTypeDefCategory = typeDef.getCategory();

            if (retrievedTypeDefCategory != null)
            {
                if (category.getTypeCode() == retrievedTypeDefCategory.getTypeCode())
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // TODO Throw logic error
            }
        }

        return false;
    }


    /**
     * Return boolean indicating if a classification type can be applied to a specified entity.  This
     * uses the list of valid entity types located in the ClassificationDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param classificationTypeName - name of the classification's type (ClassificationDef)
     * @param entityTypeName - name of the entity's type (EntityDef)
     * @return boolean indicating if the classification is valid for the entity.
     */
    public boolean    isValidClassificationForEntity(String  sourceName,
                                                     String  classificationTypeName,
                                                     String  entityTypeName)
    {
        try
        {
            if ((isValidTypeCategory(sourceName, TypeDefCategory.CLASSIFICATION_DEF, classificationTypeName)) &&
                (isValidTypeCategory(sourceName, TypeDefCategory.ENTITY_DEF, entityTypeName)))
            {
                ClassificationDef  classificationTypeDef = (ClassificationDef)knownTypes.get(classificationTypeName);

                if (classificationTypeDef != null)
                {
                    ArrayList<TypeDefLink>   entityDefs = classificationTypeDef.getValidEntityDefs();

                    if (entityDefs == null)
                    {
                        /*
                         * The classification has no restrictions on which entities it can be attached to.
                         */
                        return true;
                    }
                    else
                    {
                        /*
                         * The classification can only be attached to the entities listed.  Note an empty list
                         * means the classification can not be attached to any entity and it is effectively useless.
                         */
                        for (TypeDefLink  allowedEntity : entityDefs)
                        {
                            if (allowedEntity != null)
                            {
                                if (entityTypeName.equals(allowedEntity.getName()))
                                {
                                    return true;
                                }
                            }
                        }

                        return false;
                    }
                }
                else
                {
                    // TODO log audit record - logic error
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        catch (TypeErrorException   typeError)
        {
            // TODO log audit record - invalid Types
            return false;
        }
        catch (ClassCastException   castError)
        {
            // TODO log audit record - logic error - category not matching TypeDef instance type
            return false;
        }
    }


    /**
     * Return the list of valid InstanceStatus states that instances of this type can handle.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type
     * @return list of InstanceStatus enums
     * @throws TypeErrorException - the type name is not recognized.
     */
    public ArrayList<InstanceStatus> getValidStatusList(String  sourceName, String typeName) throws TypeErrorException
    {
        if (typeName == null)
        {
            // TODO throw TypeError Exception
        }

        TypeDef   typeDef = knownTypes.get(typeName);

        if (typeDef == null)
        {
            // TODO throw TypeError exception
        }

        return typeDef.getValidInstanceStatusList();
    }


    /**
     * Return the initial status value to use for an instance of the supplied type.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type to extract the initial status from.
     * @return InstanceStatus enum
     * @throws TypeErrorException - the type name is not recognized.
     */
    public InstanceStatus getInitialStatus(String  sourceName, String typeName) throws TypeErrorException
    {
        if (typeName == null)
        {
            // TODO throw TypeError Exception
        }

        TypeDef   typeDef = knownTypes.get(typeName);

        if (typeDef == null)
        {
            // TODO throw TypeError exception
        }

        return typeDef.getInitialStatus();
    }


    /**
     * Return the URL string to use for direct access to the metadata instance.  This can be used for
     * entities and relationships.  However, not all servers support direct access, in which case, this
     * URL is null.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guid - unique identifier for the instance.
     * @return String URL with placeholder for variables such as userId.
     */
    public String getInstanceURL(String  sourceName, String guid)
    {
        // TODO Need to work out what instance URL's look like for OMRS instances.  These URLs will be supported
        // TODO by the REST API
        return null;
    }


    /*
     * ========================
     * OMRSTypeDefHelper
     */

    /**
     * Return the TypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    public TypeDef  getTypeDefByName (String    sourceName,
                                      String    typeDefName)
    {
        return knownTypes.get(typeDefName);
    }


    /**
     * Return the AttributeTypeDef identified by the name supplied by the caller.  This is used in the connectors when
     * validating the actual types of the repository with the known open metadata types - looking specifically
     * for types of the same name but with different content.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefName - unique name for the TypeDef
     * @return AttributeTypeDef object or null if AttributeTypeDef is not known.
     */
    public AttributeTypeDef getAttributeTypeDefByName (String    sourceName,
                                                       String    attributeTypeDefName)
    {
        return knownAttributeTypes.get(attributeTypeDefName);
    }


    /**
     * Return the TypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type of a metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public TypeDef  getTypeDef (String    sourceName,
                                String    typeDefGUID,
                                String    typeDefName) throws TypeErrorException
    {
        if (validTypeId(sourceName, typeDefGUID, typeDefName))
        {
            return knownTypes.get(typeDefName);
        }
        else
        {
            return null;
        }
    }


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type definition of a metadata instance's
     * property.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @param attributeTypeDefName - unique name for the AttributeTypeDef
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public  AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                                   String    attributeTypeDefGUID,
                                                   String    attributeTypeDefName) throws TypeErrorException
    {
        return null;
    }


    /**
     * Returns an updated TypeDef that has had the supplied patch applied.  It throws an exception if any part of
     * the patch is incompatible with the original TypeDef.  For example, if there is a mismatch between
     * the type or version that either represents.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefPatch - patch to apply
     * @return updated TypeDef
     * @throws PatchErrorException - the patch is either badly formatted, or does not apply to the supplied TypeDef
     */
    public TypeDef   applyPatch(String sourceName, TypeDefPatch typeDefPatch) throws PatchErrorException
    {
        TypeDef    originalTypeDef = null;
        TypeDef    clonedTypeDef   = null;
        TypeDef    updatedTypeDef  = null;

        /*
         * Begin with simple validation of the typeDef patch.
         */
        if (typeDefPatch != null)
        {
            // TODO patch error
        }

        long newVersion = typeDefPatch.getUpdateToVersion();
        if (newVersion <= typeDefPatch.getApplyToVersion())
        {
            // TODO PatchError
        }

        TypeDefPatchAction   patchAction = typeDefPatch.getAction();
        if (patchAction == null)
        {
            // TODO patch error
        }

        /*
         * Locate the current definition for the TypeDef
         */
        try
        {
            originalTypeDef = this.getTypeDef(sourceName, typeDefPatch.getTypeDefGUID(), typeDefPatch.getTypeName());
        }
        catch (TypeErrorException   typeError)
        {
            // TODO - wrap TypeError in Patch Error
        }

        /*
         * Is the version compatible?
         */
        if (originalTypeDef.getVersion() != typeDefPatch.getApplyToVersion())
        {
            // TODO throw PatchException - incompatible versions
        }

        /*
         * OK to perform the update.  Need to create a new TypeDef object.  TypeDef is an abstract class
         * so need to use the TypeDefCategory to create a new object of the correct type.
         */
        TypeDefCategory category = originalTypeDef.getCategory();
        if (category == null)
        {
            // TODO Throw PatchError - base type is messed up
        }

        try
        {
            switch (category)
            {
                case ENTITY_DEF:
                    clonedTypeDef = new EntityDef((EntityDef) originalTypeDef);
                    break;

                case RELATIONSHIP_DEF:
                    clonedTypeDef = new RelationshipDef((RelationshipDef) originalTypeDef);
                    break;

                case CLASSIFICATION_DEF:
                    clonedTypeDef = new ClassificationDef((ClassificationDef) originalTypeDef);
                    break;
            }
        }
        catch (ClassCastException  castError)
        {
            // TODO Throw PatchError - base type is messed up
        }

        /*
         * Now we have a new TypeDef - just need to make the changes.  The Action
         */
        if (clonedTypeDef != null)
        {
            switch (patchAction)
            {
                case ADD_ATTRIBUTES:
                    updatedTypeDef = this.patchTypeDefAttributes(clonedTypeDef, typeDefPatch.getTypeDefAttributes());
                    break;

                case ADD_OPTIONS:
                    updatedTypeDef = this.patchTypeDefNewOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case UPDATE_OPTIONS:
                    updatedTypeDef = this.patchTypeDefUpdateOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case DELETE_OPTIONS:
                    updatedTypeDef = this.patchTypeDefDeleteOptions(clonedTypeDef, typeDefPatch.getTypeDefOptions());
                    break;

                case ADD_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefAddExternalStandards(clonedTypeDef,
                                                                           typeDefPatch.getExternalStandardMappings(),
                                                                           typeDefPatch.getTypeDefAttributes());
                    break;

                case UPDATE_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefUpdateExternalStandards(clonedTypeDef,
                                                                              typeDefPatch.getExternalStandardMappings(),
                                                                              typeDefPatch.getTypeDefAttributes());
                    break;

                case DELETE_EXTERNAL_STANDARDS:
                    updatedTypeDef = this.patchTypeDefDeleteExternalStandards(clonedTypeDef,
                                                                              typeDefPatch.getExternalStandardMappings(),
                                                                              typeDefPatch.getTypeDefAttributes());
                    break;

                case UPDATE_DESCRIPTIONS:
                    updatedTypeDef = this.patchTypeDefNewDescriptions(clonedTypeDef,
                                                                      typeDefPatch.getDescription(),
                                                                      typeDefPatch.getDescriptionGUID(),
                                                                      typeDefPatch.getTypeDefAttributes());
                    break;
            }
        }


        if (updatedTypeDef != null)
        {
            updatedTypeDef.setVersion(typeDefPatch.getUpdateToVersion());
            updatedTypeDef.setVersionName(typeDefPatch.getNewVersionName());
        }

        return updatedTypeDef;
    }


    /**
     * Add the supplied attributes to the properties definition for the cloned typedef.
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param typeDefAttributes - new attributes to add.
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding attributes
     */
    private TypeDef patchTypeDefAttributes(TypeDef                     clonedTypeDef,
                                           ArrayList<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        ArrayList<TypeDefAttribute>  propertyDefinitions = clonedTypeDef.getPropertiesDefinition();

        if (propertyDefinitions == null)
        {
            propertyDefinitions = new ArrayList<>();
        }

        for (TypeDefAttribute  newAttribute : typeDefAttributes)
        {
            if (newAttribute != null)
            {
                String            attributeName = newAttribute.getAttributeName();
                AttributeTypeDef  attributeType = newAttribute.getAttributeType();

                if ((attributeName != null) && (attributeType != null))
                {
                    if (propertyDefinitions.contains(newAttribute))
                    {
                        // TODO Patch error - Duplicate Attribute
                    }
                    else
                    {
                        propertyDefinitions.add(newAttribute);
                    }
                }
                else
                {
                    // TODO Patch Error - Invalid Attribute in patch
                }
            }
        }

        if (propertyDefinitions.size() > 0)
        {
            clonedTypeDef.setPropertiesDefinition(propertyDefinitions);
        }
        else
        {
            clonedTypeDef.setPropertiesDefinition(null);
        }

        return clonedTypeDef;
    }


    /**
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param typeDefOptions - new options to add
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding options
     */
    private TypeDef patchTypeDefNewOptions(TypeDef             clonedTypeDef,
                                           Map<String, String> typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param typeDefOptions - options to update
     * @return updated TypeDef
     * @throws PatchErrorException - problem updating options
     */
    private TypeDef patchTypeDefUpdateOptions(TypeDef                clonedTypeDef,
                                              Map<String, String>    typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param typeDefOptions - options to delete
     * @return updated TypeDef
     * @throws PatchErrorException - problem deleting options
     */
    private TypeDef patchTypeDefDeleteOptions(TypeDef                clonedTypeDef,
                                              Map<String, String>    typeDefOptions) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Add new mappings to external standards to the TypeDef.
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param externalStandardMappings - new mappings to add
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding mapping(s)
     */
    private TypeDef patchTypeDefAddExternalStandards(TypeDef                            clonedTypeDef,
                                                     ArrayList<ExternalStandardMapping> externalStandardMappings,
                                                     ArrayList<TypeDefAttribute>        typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Update the supplied mappings from the TypeDef.
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param externalStandardMappings - mappings to update
     * @return updated TypeDef
     * @throws PatchErrorException - problem updating mapping(s)
     */
    private TypeDef patchTypeDefUpdateExternalStandards(TypeDef                            clonedTypeDef,
                                                        ArrayList<ExternalStandardMapping> externalStandardMappings,
                                                        ArrayList<TypeDefAttribute>        typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Delete the supplied mappings from the TypeDef.
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param externalStandardMappings - list of mappings to delete
     * @return updated TypeDef
     * @throws PatchErrorException - problem deleting mapping(s)
     */
    private TypeDef patchTypeDefDeleteExternalStandards(TypeDef                            clonedTypeDef,
                                                        ArrayList<ExternalStandardMapping> externalStandardMappings,
                                                        ArrayList<TypeDefAttribute>        typeDefAttributes) throws PatchErrorException
    {
        // TODO
        return null;
    }


    /**
     * Update the descriptions for the TypeDef or any of its attributes.  If the description values are null, they are
     * not changes in the TypeDef.  This means there is no way to clear a description - just update it for a better one.
     *
     * @param clonedTypeDef - TypeDef object to update
     * @param description - new description
     * @param descriptionGUID - new unique identifier for glossary term that provides detailed description of TypeDef
     * @return updated TypeDef
     * @throws PatchErrorException - problem adding new description
     */
    private TypeDef patchTypeDefNewDescriptions(TypeDef                     clonedTypeDef,
                                                String                      description,
                                                String                      descriptionGUID,
                                                ArrayList<TypeDefAttribute> typeDefAttributes) throws PatchErrorException
    {
        if (description != null)
        {
            clonedTypeDef.setDescription(description);
        }
        if (descriptionGUID != null)
        {
            clonedTypeDef.setDescriptionGUID(descriptionGUID);
        }

        if (typeDefAttributes != null)
        {
            ArrayList<TypeDefAttribute>  propertiesDefinition = clonedTypeDef.getPropertiesDefinition();

            if (propertiesDefinition == null)
            {
                // TODO throw patch error - attempting to Patch TypeDef with no properties
            }

            for (TypeDefAttribute  patchTypeDefAttribute : typeDefAttributes)
            {
                if (patchTypeDefAttribute != null)
                {
                    String     patchTypeDefAttributeName = patchTypeDefAttribute.getAttributeName();

                    if (patchTypeDefAttributeName != null)
                    {
                        for (TypeDefAttribute  existingProperty : propertiesDefinition)
                        {
                            if (existingProperty != null)
                            {
                                if (patchTypeDefAttributeName.equals(existingProperty.getAttributeName()))
                                {

                                }
                            }
                            else
                            {
                                // TODO throw Patch Error because basic Type is messed up
                            }
                        }
                    }
                    else
                    {
                        //  TODO throw Patch Error null attribute name
                    }
                }
                else
                {
                    // TODO throw Patch Error null attribute included
                }
            }
        }

        return clonedTypeDef;
    }

    /*
     * =======================
     * OMRSTypeDefValidator
     */

    /**
     * Return a summary list of the TypeDefs supported by the local metadata repository.  This is
     * broadcast to the other servers/repositories in the cohort during the membership registration exchanges
     * managed by the cohort registries.
     *
     * @return TypeDefSummary list
     */
    public ArrayList<TypeDefSummary> getLocalTypeDefs()
    {
        ArrayList<TypeDefSummary> activeTypeDefSummaries = null;

        if (activeTypes != null)
        {
            activeTypeDefSummaries = new ArrayList<>();

            for (TypeDef activeType : activeTypes.values())
            {
                activeTypeDefSummaries.add(activeType);
            }
        }

        return activeTypeDefSummaries;
    }


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * local metadata repository.  A true response means it is ok; false means conflicts have been found.
     *
     * A valid TypeDef is one that:
     * <ul>
     *     <li>
     *         Matches name, GUID and version to a TypeDef in the local repository, or
     *     </li>
     *     <li>
     *         Is not defined in the local repository.
     *     </li>
     * </ul>
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefSummaries - list of summary information about the TypeDefs.
     */
    public void validateAgainstLocalTypeDefs(String sourceName, ArrayList<TypeDefSummary> typeDefSummaries)
    {

        // TODO if invalid typeDefs are detected, they are logged and TypeDef conflict messages are sent to
        // the typeDefEventProcessor methods to distributed
    }


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefs - list of TypeDefs.
     * @return boolean flag
     */
    public boolean   validateEnterpriseTypeDefs(String sourceName, ArrayList<TypeDef> typeDefs)
    {
        return true;
    }

    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    public boolean isOpenType(String sourceName, String   typeDefGUID, String   typeDefName)
    {
        if (isKnownType(sourceName, typeDefGUID, typeDefName))
        {
            TypeDef typeDef = knownTypes.get(typeDefName);

            if (typeDef == null)
            {
                return false;
            }

            if (openTypesOriginGUID != null)
            {
                if (openTypesOriginGUID.equals(typeDef.getOrigin()))
                {
                    return true;
                }
            }
        }

        return false;
    }


    /**
     * Return boolean indicating whether the TypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    public boolean isKnownType(String sourceName, String   typeDefGUID, String   typeDefName)
    {
        if (typeDefName == null)
        {
            return false;
        }

        TypeDef  typeDef = knownTypes.get(typeDefName);

        if (typeDef == null)
        {
            return false;
        }

        if (typeDefGUID != null)
        {
            if (typeDefGUID.equals(typeDef.getGUID()))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            return true;
        }
    }


    /**
     * Return boolean indicating whether the TypeDef is in use in the repository.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    public boolean isActiveType(String sourceName, String   typeDefGUID, String   typeDefName)
    {
        if (typeDefName == null)
        {
            return false;
        }

        TypeDef  typeDef = activeTypes.get(typeDefName);

        if (typeDef == null)
        {
            return false;
        }

        if (typeDefGUID != null)
        {
            if (typeDefGUID.equals(typeDef.getGUID()))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            return true;
        }
    }

    /**
     * Return boolean indicating whether the TypeDef identifiers are valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName)
    {
        if (typeDefName == null)
        {
            // TODO Log error
            return false;
        }

        if (typeDefGUID == null)
        {
            // TODO Log warning - probably caused by local repository connector not setting up GUID properly
            return false;
        }

        TypeDef typeDef = knownTypes.get(typeDefName);

        if (typeDef == null)
        {
            // TODO log unknown type
            return false;
        }

        if (! typeDefGUID.equals(typeDef.getGUID()))
        {
            // TODO log type mismatch
            return false;
        }

        return true;
    }

    /**
     * Return boolean indicating whether the TypeDef identifiers are valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName,
                               TypeDefCategory category)
    {
        if (! validTypeId(sourceName, typeDefGUID, typeDefName))
        {
            /*
             * Error already logged.
             */
            return false;
        }

        TypeDef          typeDef = knownTypes.get(typeDefName);
        TypeDefCategory  knownTypeDefCategory = typeDef.getCategory();

        if (knownTypeDefCategory == null)
        {
            // TODO log messed up cache
            return false;
        }

        if (category.getTypeCode() != knownTypeDefCategory.getTypeCode())
        {
            // TODO log type mismatch
            return false;
        }

        return true;
    }

    /**
     * Return boolean indicating whether the TypeDef identifiers are valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param typeDefVersion - versionName of the type
     * @param typeDefCategory - category of the instance described by this TypeDef.
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName,
                               long            typeDefVersion,
                               TypeDefCategory typeDefCategory)
    {
        if (! validTypeId(sourceName, typeDefGUID, typeDefName, typeDefCategory))
        {
            return false;
        }

        TypeDef   typeDef = knownTypes.get(typeDefName);

        if (typeDef == null)
        {
            // Log logic error
            return false;
        }

        if (typeDef.getVersion() != typeDefVersion)
        {
            // TODO log warning to say version mismatch
            return false;
        }

        return true;
    }


    /**
     * Return boolean indicating whether the supplied TypeDef is valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDef - TypeDef to test
     * @return boolean result
     */
    public boolean validTypeDef(String         sourceName,
                                TypeDef        typeDef)
    {
        if (typeDef == null)
        {
            // TODO log null TypeDef
            return false;
        }

        if (validTypeId(sourceName,
                        typeDef.getGUID(),
                        typeDef.getName(),
                        typeDef.getVersion(),
                        typeDef.getCategory()))
        {
            return true;
        }
        else
        {
            return false;
        }
    }


    /**
     * Return boolean indicating whether the supplied TypeDefSummary is valid or not.
     *
     * @param sourceName - source of the TypeDefSummary (used for logging)
     * @param typeDefSummary - TypeDefSummary to test.
     * @return boolean result.
     */
    public boolean validTypeDefSummary(String                sourceName,
                                       TypeDefSummary        typeDefSummary)
    {
        if (typeDefSummary != null)
        {
            if (validTypeId(sourceName,
                            typeDefSummary.getGUID(),
                            typeDefSummary.getName(),
                            typeDefSummary.getVersion(),
                            typeDefSummary.getCategory()))
            {
                return true;
            }
        }

        return false;
    }


    /*
     * ===========================
     * OMRSTypeDefEventProcessor
     */


    /**
     * A new TypeDef has been defined either in an archive, or in another member of the cohort.
     *
     * This new TypeDef can be added to the repository if it does not clash with an existing typeDef and the local
     * repository supports dynamic type definitions.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDef - details of the new TypeDef
     */
    public void processNewTypeDefEvent(String       sourceName,
                                       String       originatorMetadataCollectionId,
                                       String       originatorServerName,
                                       String       originatorServerType,
                                       String       originatorOrganizationName,
                                       TypeDef      typeDef)
    {


        OMRSMetadataCollection metadataCollection = null;

        if (localRepositoryConnector != null)
        {
            localRepositoryConnector.getMetadataCollection();
        }

        if (metadataCollection != null)
        {
            try
            {
                /*
                 * VerifyTypeDef returns true if the typeDef is known and matches the supplied definition.
                 * It returns false if the type is supportable but has not yet been defined.
                 * It throws TypeDefNotSupportedException if the typeDef is not supported and can not
                 * be dynamically defined by the local repository.
                 */
                if (! metadataCollection.verifyTypeDef(null, typeDef))
                {
                    metadataCollection.addTypeDef(null, typeDef);

                    /*
                     * Update the active TypeDefs as this new TypeDef has been accepted by the local repository.
                     */
                    activeTypes.put(typeDef.getName(), typeDef);
                }
            }
            catch (TypeDefNotSupportedException fixedTypeSystemResponse)
            {


                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because repository does not support dynamic type definitions", typeDef);
                    log.debug("TypeDefNotSupportedException:", fixedTypeSystemResponse);

                }
            }
            catch (RepositoryErrorException error)
            {
                // TODO log an error to say that the repository is not available

                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because repository is not available", typeDef);
                    log.debug("RepositoryErrorException:", error);

                }
            }
            catch (TypeDefConflictException error)
            {
                // TODO log an error to say that the TypeDef conflicts with a TypeDef already stored.

                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because it conflicts with another TypeDef already in the repository", typeDef);
                    log.debug("TypeDefConflictException:", error);
                }
            }
            catch (InvalidTypeDefException error)
            {
                // TODO log an error to say that the TypeDef contains bad values.

                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because repository is not available", typeDef);
                    log.debug("InvalidTypeDefException:", error);
                }
            }
            catch (TypeDefKnownException error)
            {
                // TODO log an error to say that a logic error has occurred

                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because repository has a logic error", typeDef);
                    log.debug("TypeDefKnownException:", error);

                }
            }
            catch (Throwable  error)
            {
                // TODO log an error to say that an unexpected error has occurred

                if (log.isDebugEnabled())
                {
                    log.debug("TypeDef not added because repository has an unexpected error", typeDef);
                    log.debug("Throwable:", error);
                }
            }
        }
    }


    /**
     * A new AttributeTypeDef has been defined in an open metadata repository.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param attributeTypeDef - details of the new AttributeTypeDef.
     */
    public void processNewAttributeTypeDefEvent(String           sourceName,
                                                String           originatorMetadataCollectionId,
                                                String           originatorServerName,
                                                String           originatorServerType,
                                                String           originatorOrganizationName,
                                                AttributeTypeDef attributeTypeDef)
    {
        // TODO
    }


    /**
     * An existing TypeDef has been updated in a remote metadata repository.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefPatch - details of the new versionName of the TypeDef
     */
    public void processUpdatedTypeDefEvent(String       sourceName,
                                           String       originatorMetadataCollectionId,
                                           String       originatorServerName,
                                           String       originatorServerType,
                                           String       originatorOrganizationName,
                                           TypeDefPatch typeDefPatch)
    {
        OMRSMetadataCollection metadataCollection = localRepositoryConnector.getMetadataCollection();

        if (metadataCollection != null)
        {
            try
            {
                TypeDef updatedTypeDef = metadataCollection.updateTypeDef(null, typeDefPatch);

                if (log.isDebugEnabled())
                {
                    log.debug("Patch successfully applied", updatedTypeDef);
                }
            }
            catch (RepositoryErrorException  error)
            {
                // TODO log an error to say that the repository is not available

                if (log.isDebugEnabled())
                {
                    log.debug("Patch not applied because repository is not available", typeDefPatch);
                }
            }
            catch (TypeDefNotKnownException  error)
            {
                // TODO log an error to say that the TypeDef is not known

                if (log.isDebugEnabled())
                {
                    log.debug("Patch not applied because TypeDef does not exist", typeDefPatch);
                    log.debug("TypeDefNotKnownException:", error);
                }
            }
            catch (PatchErrorException  error)
            {
                // TODO log an error to say that the TypeDef patch is invalid

                if (log.isDebugEnabled())
                {
                    log.debug("Patch not applied because it is invalid", typeDefPatch);
                    log.debug("PatchErrorException:", error);
                }
            }
            catch (Throwable error)
            {
                // TODO log a generic error

                if (log.isDebugEnabled())
                {
                    log.debug("Patch not applied because of an error", typeDefPatch);
                    log.debug("Throwable:", error);
                }
            }
        }
    }


    /**
     * An existing TypeDef has been deleted in a remote metadata repository.  Both the name and the
     * GUID are provided to ensure the right TypeDef is deleted in other cohort member repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     */
    public void processDeletedTypeDefEvent(String      sourceName,
                                           String      originatorMetadataCollectionId,
                                           String      originatorServerName,
                                           String      originatorServerType,
                                           String      originatorOrganizationName,
                                           String      typeDefGUID,
                                           String      typeDefName)
    {
        // TODO
    }


    /**
     * An existing AttributeTypeDef has been deleted in an open metadata repository.  Both the name and the
     * GUID are provided to ensure the right AttributeTypeDef is deleted in other cohort member repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param attributeTypeDefGUID - unique identifier of the AttributeTypeDef
     * @param attributeTypeDefName - unique name of the AttributeTypeDef
     */
    public void processDeletedAttributeTypeDefEvent(String      sourceName,
                                                    String      originatorMetadataCollectionId,
                                                    String      originatorServerName,
                                                    String      originatorServerType,
                                                    String      originatorOrganizationName,
                                                    String      attributeTypeDefGUID,
                                                    String      attributeTypeDefName)
    {
        // TODO
    }


    /**
     * Process an event that changes either the name or guid of a TypeDef.  It is resolving a Conflicting TypeDef Error.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary - details of the original TypeDef
     * @param typeDef - updated TypeDef with new identifiers inside.
     */
    public void processReIdentifiedTypeDefEvent(String         sourceName,
                                                String         originatorMetadataCollectionId,
                                                String         originatorServerName,
                                                String         originatorServerType,
                                                String         originatorOrganizationName,
                                                TypeDefSummary originalTypeDefSummary,
                                                TypeDef        typeDef)
    {

    }


    /**
     * Process an event that changes either the name or guid of an AttributeTypeDef.
     * It is resolving a Conflicting AttributeTypeDef Error.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalAttributeTypeDef - description of original AttributeTypeDef
     * @param attributeTypeDef - updated AttributeTypeDef with new identifiers inside.
     */
    public void processReIdentifiedAttributeTypeDefEvent(String           sourceName,
                                                         String           originatorMetadataCollectionId,
                                                         String           originatorServerName,
                                                         String           originatorServerType,
                                                         String           originatorOrganizationName,
                                                         AttributeTypeDef originalAttributeTypeDef,
                                                         AttributeTypeDef attributeTypeDef)
    {
        // TODO
    }


    /**
     * Process a detected conflict in type definitions (TypeDefs) used in the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originatorTypeDefSummary - details of the TypeDef in the event originator
     * @param otherMetadataCollectionId - the metadataCollection using the conflicting TypeDef
     * @param conflictingTypeDefSummary - the details of the TypeDef in the other metadata collection
     * @param errorMessage - details of the error that occurs when the connection is used.
     */
    public void processTypeDefConflictEvent(String         sourceName,
                                            String         originatorMetadataCollectionId,
                                            String         originatorServerName,
                                            String         originatorServerType,
                                            String         originatorOrganizationName,
                                            TypeDefSummary originatorTypeDefSummary,
                                            String         otherMetadataCollectionId,
                                            TypeDefSummary conflictingTypeDefSummary,
                                            String         errorMessage)
    {
        // TODO
    }


    /**
     * Process a detected conflict in the attribute type definitions (AttributeTypeDefs) used in the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originatorAttributeTypeDef- description of the AttributeTypeDef in the event originator.
     * @param otherMetadataCollectionId - the metadataCollection using the conflicting AttributeTypeDef.
     * @param conflictingAttributeTypeDef - description of the AttributeTypeDef in the other metadata collection.
     * @param errorMessage - details of the error that occurs when the connection is used.
     */
    public void processAttributeTypeDefConflictEvent(String           sourceName,
                                                     String           originatorMetadataCollectionId,
                                                     String           originatorServerName,
                                                     String           originatorServerType,
                                                     String           originatorOrganizationName,
                                                     AttributeTypeDef originatorAttributeTypeDef,
                                                     String           otherMetadataCollectionId,
                                                     AttributeTypeDef conflictingAttributeTypeDef,
                                                     String           errorMessage)
    {
        // TODO
    }


    /**
     * A TypeDef from another member in the cohort is at a different versionName than the local repository.  This may
     * create some inconsistencies in the different copies of instances of this type in different members of the
     * cohort.  The recommended action is to update all TypeDefs to the latest versionName.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - identifier of the metadata collection that is reporting a TypeDef at a
     *                                   different level to the local repository.
     * @param targetTypeDefSummary - details of the target TypeDef
     * @param otherTypeDef - details of the TypeDef in the local repository.
     */
    public void processTypeDefPatchMismatchEvent(String         sourceName,
                                                 String         originatorMetadataCollectionId,
                                                 String         originatorServerName,
                                                 String         originatorServerType,
                                                 String         originatorOrganizationName,
                                                 String         targetMetadataCollectionId,
                                                 TypeDefSummary targetTypeDefSummary,
                                                 TypeDef        otherTypeDef,
                                                 String         errorMessage)
    {

    }


    /*
     * =====================
     * OMRSInstanceValidator
     */

    /**
     * Test that the supplied entity is valid.
     *
     * @param sourceName - source of the entity (used for logging)
     * @param entity - entity to test
     * @return boolean result
     */
    public boolean validEntity(String       sourceName,
                               EntityDetail entity)
    {
        if (entity == null)
        {
            // TODO log null entity
            return false;
        }

        InstanceType instanceType = entity.getType();

        if (instanceType == null)
        {
            // TODO log null type
            return false;
        }

        if (! validInstanceId(sourceName,
                              instanceType.getTypeDefGUID(),
                              instanceType.getTypeDefName(),
                              instanceType.getTypeDefCategory(),
                              entity.getGUID()))
        {
            /*
             * Error messages already logged.
             */
            return false;
        }

        String          homeMetadataCollectionId = entity.getMetadataCollectionId();

        if (homeMetadataCollectionId == null)
        {
            // TODO log error
            return false;
        }

        return true;
    }


    /**
     * Test that the supplied relationship is valid.
     *
     * @param sourceName - source of the relationship (used for logging)
     * @param relationship - relationship to test
     * @return boolean result
     */
    public boolean validRelationship(String       sourceName,
                                     Relationship relationship)
    {
        if (relationship == null)
        {
            // TODO log null relationship
            return false;
        }

        InstanceType instanceType = relationship.getType();

        if (instanceType == null)
        {
            // TODO log null type
            return false;
        }

        if (! validInstanceId(sourceName,
                              instanceType.getTypeDefGUID(),
                              instanceType.getTypeDefName(),
                              instanceType.getTypeDefCategory(),
                              relationship.getGUID()))
        {
            /*
             * Error messages already logged.
             */
            return false;
        }

        String          homeMetadataCollectionId = relationship.getMetadataCollectionId();

        if (homeMetadataCollectionId == null)
        {
            // TODO log error
            return false;
        }

        return true;
    }


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
    public boolean validInstanceId(String           sourceName,
                                   String           typeDefGUID,
                                   String           typeDefName,
                                   TypeDefCategory  category,
                                   String           instanceGUID)
    {
        if (instanceGUID == null)
        {
            // TODO - log null guid
            return false;
        }

        if (! validTypeId(sourceName,
                          typeDefGUID,
                          typeDefName,
                          category))
        {
            /*
             * Error messages already logged
             */
            return false;
        }

        return true;
    }
}
