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
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceStatus;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.apache.atlas.omrs.rest.server.OMRSRepositoryRESTServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
                                                     OMRSTypeDefValidator
{
    private LocalOMRSRepositoryConnector      localRepositoryConnector       = null;
    private OMRSRepositoryEventManager        outboundRepositoryEventManager = null;
    private OMRSRepositoryConnector           realLocalConnector             = null;
    private OMRSRepositoryEventExchangeRule   saveExchangeRule               = null;
    private String                            openTypesOriginGUID            = null;
    private HashMap<String, TypeDef>          knownTypeDefGUIDs              = new HashMap<>();
    private HashMap<String, TypeDef>          knownTypeDefNames              = new HashMap<>();
    private HashMap<String, AttributeTypeDef> knownAttributeTypeDefGUIDs     = new HashMap<>();
    private HashMap<String, AttributeTypeDef> knownAttributeTypeDefNames     = new HashMap<>();
    private HashMap<String, TypeDef>          activeTypeDefGUIDs             = new HashMap<>();
    private HashMap<String, TypeDef>          activeTypeDefNames             = new HashMap<>();
    private HashMap<String, AttributeTypeDef> activeAttributeTypeDefGUIDs    = new HashMap<>();
    private HashMap<String, AttributeTypeDef> activeAttributeTypeDefNames    = new HashMap<>();


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
        this.localRepositoryConnector = localRepositoryConnector;
        this.realLocalConnector = realLocalConnector;
        this.saveExchangeRule = saveExchangeRule;
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
            knownTypeDefGUIDs.put(newTypeDef.getGUID(), newTypeDef);
            knownTypeDefNames.put(newTypeDef.getName(), newTypeDef);

            if (localRepositoryConnector != null)
            {
                activeTypeDefGUIDs.put(newTypeDef.getGUID(), newTypeDef);
                activeTypeDefNames.put(newTypeDef.getName(), newTypeDef);

                if (log.isDebugEnabled())
                {
                    log.debug("New Active Type " + newTypeDef.getName() + " from " + sourceName, newTypeDef);
                }
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
        if (this.validAttributeTypeDef(sourceName, newAttributeTypeDef))
        {
            knownAttributeTypeDefGUIDs.put(newAttributeTypeDef.getGUID(), newAttributeTypeDef);
            knownAttributeTypeDefNames.put(newAttributeTypeDef.getName(), newAttributeTypeDef);

            if (localRepositoryConnector != null)
            {
                activeAttributeTypeDefGUIDs.put(newAttributeTypeDef.getGUID(), newAttributeTypeDef);
                activeAttributeTypeDefNames.put(newAttributeTypeDef.getName(), newAttributeTypeDef);

                if (log.isDebugEnabled())
                {
                    log.debug("New Active Attribute Type " + newAttributeTypeDef.getName() + " from " + sourceName, newAttributeTypeDef);
                }
            }
        }
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
            knownTypeDefGUIDs.put(typeDef.getGUID(), typeDef);
            knownTypeDefNames.put(typeDef.getName(), typeDef);

            if (localRepositoryConnector != null)
            {
                activeTypeDefGUIDs.put(typeDef.getGUID(), typeDef);
                activeTypeDefNames.put(typeDef.getName(), typeDef);

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
            knownTypeDefGUIDs.remove(obsoleteTypeDefGUID);
            knownTypeDefNames.remove(obsoleteTypeDefName);

            if (localRepositoryConnector != null)
            {
                activeTypeDefGUIDs.remove(obsoleteTypeDefGUID);
                activeTypeDefNames.remove(obsoleteTypeDefName);

                if (log.isDebugEnabled())
                {
                    log.debug("Deleted Active TypeDef " + obsoleteTypeDefName + " from " + sourceName);
                }
            }
        }
    }


    /**
     * Delete a cached AttributeTypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param obsoleteAttributeTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteAttributeTypeDefName - String unique name for the AttributeTypeDef.
     */
    public void deleteAttributeTypeDef(String    sourceName,
                                       String    obsoleteAttributeTypeDefGUID,
                                       String    obsoleteAttributeTypeDefName)
    {
        if (this.validTypeId(sourceName, obsoleteAttributeTypeDefGUID, obsoleteAttributeTypeDefName))
        {
            knownAttributeTypeDefGUIDs.remove(obsoleteAttributeTypeDefGUID);
            knownAttributeTypeDefNames.remove(obsoleteAttributeTypeDefName);

            if (localRepositoryConnector != null)
            {
                activeAttributeTypeDefGUIDs.remove(obsoleteAttributeTypeDefGUID);
                activeAttributeTypeDefNames.remove(obsoleteAttributeTypeDefName);

                if (log.isDebugEnabled())
                {
                    log.debug("Deleted Active AttributeTypeDef " + obsoleteAttributeTypeDefName + " from " + sourceName);
                }
            }
        }
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
        this.deleteTypeDef(sourceName, originalTypeDefGUID, originalTypeDefName);
        this.addTypeDef(sourceName, newTypeDef);
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
        this.deleteAttributeTypeDef(sourceName, originalAttributeTypeDefGUID, originalAttributeTypeDefName);
        this.addAttributeTypeDef(sourceName, newAttributeTypeDef);
    }


    /**
     * Return the list of property names defined for this TypeDef.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDef - type definition to work with.
     * @return list of String property names
     * @throws TypeErrorException - there is an issue with the TypeDef.
     */
    private List<String>  getPropertyNames(String sourceName, TypeDef   typeDef) throws TypeErrorException
    {
        final  String                  methodName = "getPropertyNames()";
        List<String>              propertyNames = null;

        if (validTypeDef(sourceName, typeDef))
        {
            List<TypeDefAttribute>    propertiesDefinition = typeDef.getPropertiesDefinition();

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
     * @param methodName - name of calling method.
     * @return InstanceType object containing TypeDef properties such as unique identifier (guid),
     *                             typeDef name and version name
     * @throws TypeErrorException - the type name is not a recognized type or is of the wrong category or there is
     *                              a problem with the cached TypeDef.
     */
    public InstanceType getInstanceType(String           sourceName,
                                        TypeDefCategory  category,
                                        String           typeName,
                                        String           methodName) throws TypeErrorException
    {
        final String thisMethodName = "getInstanceType";

        if (isValidTypeCategory(sourceName, category, typeName, methodName))
        {
            TypeDef typeDef = knownTypeDefNames.get(typeName);

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
                List<String>      propertyNames = this.getPropertyNames(sourceName, typeDef);

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
                List<TypeDefLink>      superTypes    = new ArrayList<>();
                TypeDefLink            superTypeLink = typeDef.getSuperType();

                while (superTypeLink != null)
                {
                    String             superTypeName = superTypeLink.getName();

                    if (superTypeName != null)
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug(typeName + " from " + sourceName + " has super type " + superTypeName);
                        }

                        /*
                         * Save the name of the super type into the instance type
                         */
                        superTypes.add(superTypeLink);

                        /*
                         * Retrieve the TypeDef for this super type
                         */
                        TypeDef         superTypeDef  = knownTypeDefNames.get(superTypeName);

                        if (superTypeDef != null)
                        {
                            List<String>      superTypePropertyNames = this.getPropertyNames(sourceName, superTypeDef);

                            if (superTypePropertyNames != null)
                            {
                                propertyNames.addAll(0, superTypePropertyNames);
                            }

                            /*
                             * Retrieve the super type for this typeDef.  It will be null if the type is top-level.
                             */
                            superTypeLink = superTypeDef.getSuperType();
                        }
                        else
                        {
                            /*
                             * Super type not known so stop processing
                             */
                            log.error(superTypeName + " is not known");
                            throwContentManagerLogicError(sourceName, methodName, thisMethodName);
                        }
                    }
                    else
                    {
                        /*
                         * Super type is invalid - corrupted cache
                         */
                        log.error("Corrupted TypeDef cache");
                        throwContentManagerLogicError(sourceName, methodName, thisMethodName);
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

                return instanceType;
            }
            else
            {
                log.error("TypeDef " + typeName + " already validated");
                throwContentManagerLogicError(sourceName, methodName, thisMethodName);
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
                                          String            typeName,
                                          String            methodName) throws TypeErrorException
    {
        final String  thisMethodName = "isValidTypeCategory";

        if (category == null)
        {
            throwContentManagerLogicError(sourceName, methodName, thisMethodName);
            return false;
        }

        if (typeName == null)
        {
            throwContentManagerLogicError(sourceName, methodName, thisMethodName);
            return false;
        }

        TypeDef   typeDef = knownTypeDefNames.get(typeName);

        if (typeDef != null)
        {
            TypeDefCategory  retrievedTypeDefCategory = typeDef.getCategory();

            if (retrievedTypeDefCategory != null)
            {
                return (category.getTypeCode() == retrievedTypeDefCategory.getTypeCode());
            }
            else
            {
                throwContentManagerLogicError(sourceName, methodName, thisMethodName);
            }
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN_FOR_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName,
                                                         category.getTypeName(),
                                                         methodName,
                                                         sourceName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
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
     * @param methodName - name of calling method.
     * @return boolean indicating if the classification is valid for the entity.
     */
    public boolean    isValidClassificationForEntity(String  sourceName,
                                                     String  classificationTypeName,
                                                     String  entityTypeName,
                                                     String  methodName)
    {
        final String  thisMethodName = "isValidClassificationForEntity";

        try
        {
            if ((isValidTypeCategory(sourceName, TypeDefCategory.CLASSIFICATION_DEF, classificationTypeName, methodName)) &&
                (isValidTypeCategory(sourceName, TypeDefCategory.ENTITY_DEF, entityTypeName, methodName)))
            {
                ClassificationDef  classificationTypeDef = (ClassificationDef) knownTypeDefNames.get(classificationTypeName);

                if (classificationTypeDef != null)
                {
                    List<TypeDefLink>   entityDefs = classificationTypeDef.getValidEntityDefs();

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
                    throwContentManagerLogicError(sourceName, methodName, thisMethodName);
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
            throwContentManagerLogicError(sourceName, methodName, thisMethodName);
            return false;
        }
        catch (ClassCastException   castError)
        {
            throwContentManagerLogicError(sourceName, methodName, thisMethodName);
            return false;
        }
    }


    /**
     * Return the requested type.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type
     * @param thisMethodName - name of calling method.
     * @param originalMethodName - name of original calling method.
     * @return list of InstanceStatus enums
     * @throws TypeErrorException - the type name is not recognized.
     */
    private TypeDef getTypeDefFromCache(String  sourceName,
                                        String  typeName,
                                        String  thisMethodName,
                                        String  originalMethodName) throws TypeErrorException
    {
        if (typeName == null)
        {
            this.throwContentManagerLogicError(sourceName, thisMethodName, originalMethodName);
        }

        TypeDef   typeDef = knownTypeDefNames.get(typeName);

        if (typeDef == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeName, originalMethodName, sourceName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         originalMethodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
        }

        return typeDef;
    }


    /**
     * Return the list of valid InstanceStatus states that instances of this type can handle.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type
     * @param methodName - name of calling method.
     * @return list of InstanceStatus enums
     * @throws TypeErrorException - the type name is not recognized.
     */
    public List<InstanceStatus> getValidStatusList(String  sourceName,
                                                   String  typeName,
                                                   String  methodName) throws TypeErrorException
    {
        final String thisMethodName = "validStatusList";

        TypeDef   typeDef = this.getTypeDefFromCache(sourceName, typeName, thisMethodName, methodName);

        return typeDef.getValidInstanceStatusList();
    }


    /**
     * Return the initial status value to use for an instance of the supplied type.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeName - name of the type to extract the initial status from.
     * @param methodName - calling method
     * @return InstanceStatus enum
     * @throws TypeErrorException - the type name is not recognized.
     */
    public InstanceStatus getInitialStatus(String sourceName,
                                           String typeName,
                                           String methodName) throws TypeErrorException
    {
        final String thisMethodName = "getInitialStatus";

        TypeDef   typeDef = this.getTypeDefFromCache(sourceName, typeName, thisMethodName, methodName);

        return typeDef.getInitialStatus();
    }


    /**
     * Return the URL string to use for direct access to the metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guid - unique identifier for the instance.
     * @return String URL with placeholder for variables such as userId.
     */
    public String getEntityURL(String  sourceName, String guid)
    {
        return OMRSRepositoryRESTServices.getEntityURL(guid);
    }


    /**
     * Return the URL string to use for direct access to the metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guid - unique identifier for the instance.
     * @return String URL with placeholder for variables such as userId.
     */
    public String getRelationshipURL(String  sourceName, String guid)
    {
        return OMRSRepositoryRESTServices.getRelationshipURL(guid);
    }


    /*
     * ========================
     * OMRSTypeDefHelper
     */


    /**
     * Return the list of typedefs known by the local repository.
     *
     * @return TypeDef gallery
     */
    public TypeDefGallery   getActiveTypeDefGallery()
    {
        TypeDefGallery               typeDefGallery               = new TypeDefGallery();

        if (! activeAttributeTypeDefNames.isEmpty())
        {
            typeDefGallery.setAttributeTypeDefs(new ArrayList<>(activeAttributeTypeDefNames.values()));
        }

        if (! activeTypeDefNames.isEmpty())
        {
            typeDefGallery.setTypeDefs(new ArrayList<>(activeTypeDefNames.values()));
        }

        return typeDefGallery;
    }


    /**
     * Return the list of typedefs known by the local repository.
     *
     * @return TypeDef gallery
     */
    public TypeDefGallery   getKnownTypeDefGallery()
    {
        TypeDefGallery               typeDefGallery               = new TypeDefGallery();

        if (! knownAttributeTypeDefNames.isEmpty())
        {
            typeDefGallery.setAttributeTypeDefs(new ArrayList<>(knownAttributeTypeDefNames.values()));
        }

        if (! knownTypeDefNames.isEmpty())
        {
            typeDefGallery.setTypeDefs(new ArrayList<>(knownTypeDefNames.values()));
        }

        return typeDefGallery;
    }


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
        return knownTypeDefNames.get(typeDefName);
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
        return knownAttributeTypeDefNames.get(attributeTypeDefName);
    }


    /**
     * Return the TypeDefs identified by the name supplied by the caller.  The TypeDef name may have wild
     * card characters in it which is why the results are returned in a list.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefName - unique name for the TypeDef
     * @return TypeDef object or null if TypeDef is not known.
     */
    public TypeDefGallery getActiveTypesByWildCardName (String    sourceName,
                                                        String    typeDefName)
    {
        if (typeDefName != null)
        {
            Collection<TypeDef>   typeDefs       = activeTypeDefNames.values();

            List<TypeDef>         matchedTypeDefs = new ArrayList<>();
            for (TypeDef typeDef : typeDefs)
            {
                if (typeDef != null)
                {
                    if (typeDef.getName().matches(typeDefName))
                    {
                        matchedTypeDefs.add(typeDef);
                    }
                }
            }

            Collection<AttributeTypeDef>   attributeTypeDefs        = activeAttributeTypeDefNames.values();
            List<AttributeTypeDef>         matchedAttributeTypeDefs = new ArrayList<>();

            for (AttributeTypeDef attributeTypeDef : attributeTypeDefs)
            {
                if (attributeTypeDef != null)
                {
                    if (attributeTypeDef.getName().matches(typeDefName))
                    {
                        matchedAttributeTypeDefs.add(attributeTypeDef);
                    }
                }
            }

            if ((! matchedTypeDefs.isEmpty()) || (! matchedAttributeTypeDefs.isEmpty()))
            {
                TypeDefGallery        typeDefGallery = new TypeDefGallery();

                if (! matchedTypeDefs.isEmpty())
                {
                    typeDefGallery.setTypeDefs(matchedTypeDefs);
                }
                else
                {
                    typeDefGallery.setTypeDefs(null);
                }

                if (! matchedAttributeTypeDefs.isEmpty())
                {
                    typeDefGallery.setAttributeTypeDefs(matchedAttributeTypeDefs);
                }
                else
                {
                    typeDefGallery.setAttributeTypeDefs(null);
                }

                return typeDefGallery;
            }
        }

        return null;
    }


    /**
     * Return the TypeDef identified by the guid supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public TypeDef  getTypeDef (String    sourceName,
                                String    guidParameterName,
                                String    typeDefGUID,
                                String    methodName) throws TypeErrorException
    {
        final String thisMethodName = "getTypeDef";

        if (typeDefGUID != null)
        {
            TypeDef typeDef = knownTypeDefGUIDs.get(typeDefGUID);

            if (typeDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(typeDefGUID,
                                                                                                         guidParameterName,
                                                                                                         methodName,
                                                                                                         sourceName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                             this.getClass().getName(),
                                             methodName,
                                             errorMessage,
                                             errorCode.getSystemAction(),
                                             errorCode.getUserAction());
            }

            return typeDef;
        }

        throwContentManagerLogicError(sourceName, methodName, thisMethodName);
        return null;
    }


    /**
     * Return the AttributeTypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that only the guid is known.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier for the AttributeTypeDef
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                                  String    attributeTypeDefGUID,
                                                  String    methodName) throws TypeErrorException
    {
        final String thisMethodName = "getAttributeTypeDef";

        if (attributeTypeDefGUID != null)
        {
            AttributeTypeDef attributeTypeDef = knownAttributeTypeDefGUIDs.get(attributeTypeDefGUID);

            if (attributeTypeDef == null)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                             this.getClass().getName(),
                                             methodName,
                                             errorMessage,
                                             errorCode.getSystemAction(),
                                             errorCode.getUserAction());
            }

            return attributeTypeDef;
        }

        throwContentManagerLogicError(sourceName, methodName, thisMethodName);
        return null;
    }

    /**
     * Return the TypeDef identified by the guid and name supplied by the caller.  This call is used when
     * retrieving a type that should exist.  For example, retrieving the type of a metadata instance.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier for the TypeDef
     * @param typeDefName - unique name for the TypeDef
     * @param methodName - calling method
     * @return TypeDef object or null if the
     * @throws TypeErrorException - invalid type
     */
    public TypeDef  getTypeDef (String    sourceName,
                                String    guidParameterName,
                                String    nameParameterName,
                                String    typeDefGUID,
                                String    typeDefName,
                                String    methodName) throws TypeErrorException
    {
        if (validTypeId(sourceName, typeDefGUID, typeDefName))
        {
            return knownTypeDefNames.get(typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.BAD_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
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
     * @param methodName - calling method
     * @return TypeDef object
     * @throws TypeErrorException - unknown or invalid type
     */
    public  AttributeTypeDef  getAttributeTypeDef (String    sourceName,
                                                   String    attributeTypeDefGUID,
                                                   String    attributeTypeDefName,
                                                   String    methodName) throws TypeErrorException
    {
        if (validTypeId(sourceName, attributeTypeDefGUID, attributeTypeDefName))
        {
            return knownAttributeTypeDefNames.get(attributeTypeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.BAD_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                                         this.getClass().getName(),
                                         methodName,
                                         errorMessage,
                                         errorCode.getSystemAction(),
                                         errorCode.getUserAction());
        }
    }


    /*
     * =======================
     * OMRSTypeDefValidator
     */


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefs - list of TypeDefs.
     * @throws RepositoryErrorException - a conflicting or invalid TypeDef has been returned
     */
    public void   validateEnterpriseTypeDefs(String        sourceName,
                                             List<TypeDef> typeDefs,
                                             String        methodName) throws RepositoryErrorException
    {
        for (TypeDef typeDef : typeDefs)
        {
            if (validTypeId(sourceName, typeDef.getGUID(), typeDef.getName()))
            {
                if (!isKnownType(sourceName, typeDef.getGUID(), typeDef.getName()))
                {
                    knownTypeDefNames.put(typeDef.getName(), typeDef);
                }
            }
            else
            {
                OMRSErrorCode errorCode    = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
    }


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param attributeTypeDefs - list of AttributeTypeDefs.
     * @throws RepositoryErrorException - a conflicting or invalid AttributeTypeDef has been returned
     */
    public void   validateEnterpriseAttributeTypeDefs(String                 sourceName,
                                                      List<AttributeTypeDef> attributeTypeDefs,
                                                      String                 methodName) throws RepositoryErrorException
    {
        for (AttributeTypeDef attributeTypeDef : attributeTypeDefs)
        {
            if (validTypeId(sourceName, attributeTypeDef.getGUID(), attributeTypeDef.getName()))
            {
                if (!isKnownType(sourceName, attributeTypeDef.getGUID(), attributeTypeDef.getName()))
                {
                    knownAttributeTypeDefNames.put(attributeTypeDef.getName(), attributeTypeDef);
                }
            }
            else
            {
                OMRSErrorCode errorCode    = OMRSErrorCode.CONFLICTING_ENTERPRISE_TYPEDEFS;
                String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                   this.getClass().getName(),
                                                   methodName,
                                                   errorMessage,
                                                   errorCode.getSystemAction(),
                                                   errorCode.getUserAction());
            }
        }
    }


    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    public boolean isOpenType(String sourceName, String typeGUID, String typeName)
    {
        if (validTypeId(sourceName, typeGUID, typeName))
        {
            TypeDef typeDef = knownTypeDefNames.get(typeName);

            if (typeDef == null)
            {
                return false;
            }

            if (openTypesOriginGUID != null)
            {
                if (openTypesOriginGUID.equals(typeDef.getOrigin()))
                {
                    log.debug("TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName + " is an open type");
                    return true;
                }
                else
                {
                    log.debug("TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName + " is NOT an open type");
                }
            }
        }

        return false;
    }


    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    public boolean isOpenTypeId(String  sourceName, String   typeGUID)
    {
        if (typeGUID != null)
        {
            TypeDef typeDef = knownTypeDefGUIDs.get(typeGUID);
            if (typeDef != null)
            {
                String originGUID = typeDef.getOrigin();

                if (originGUID != null)
                {
                    if (originGUID.equals(openTypesOriginGUID))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }


    /**
     * Return boolean indicating whether the (AttributeTypeDef/TypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    public boolean isKnownType(String sourceName, String typeGUID, String typeName)
    {
        if (this.validTypeId(sourceName, typeGUID, typeName))
        {
            TypeDef  typeDef = knownTypeDefNames.get(typeName);

            if (typeDef == null)
            {
                AttributeTypeDef  attributeTypeDef = knownAttributeTypeDefNames.get(typeName);

                if (attributeTypeDef == null)
                {
                    log.debug("Unknown (Attribute)TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                    return false;
                }
                else
                {
                    log.debug("Known AttributeTypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                    return true;
                }
            }
            else
            {
                log.debug("Known TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                return true;
            }
        }
        else
        {
            log.error("Invalid TypeDef  from " + sourceName + " so can not validate known type");

            return false;
        }
    }


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    public boolean isKnownTypeId(String  sourceName, String   typeGUID)
    {
        if (typeGUID != null)
        {
            if (knownTypeDefGUIDs.get(typeGUID) != null)
            {
                return true;
            }

            if (knownAttributeTypeDefGUIDs.get(typeGUID) != null)
            {
                return true;
            }
        }

        return false;
    }


    /**
     * Return boolean indicating whether the TypeDef is in use in the repository.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    public boolean isActiveType(String sourceName, String typeGUID, String typeName)
    {
        if (this.validTypeId(sourceName, typeGUID, typeName))
        {
            TypeDef  typeDef = activeTypeDefNames.get(typeName);

            if (typeDef == null)
            {
                AttributeTypeDef  attributeTypeDef = activeAttributeTypeDefNames.get(typeName);

                if (attributeTypeDef == null)
                {
                    log.debug("Inactive (Attribute)TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                    return false;
                }
                else
                {
                    log.debug("Active AttributeTypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                    return true;
                }
            }
            else
            {
                log.debug("Active TypeDef " + typeName + " (GUID = " + typeGUID + ") from " + sourceName);
                return true;
            }
        }
        else
        {
            log.error("Invalid TypeDef  from " + sourceName + " so can not validate active type");

            return false;
        }
    }


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is in use in the local repository.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    public boolean isActiveTypeId(String  sourceName, String   typeGUID)
    {
        if (typeGUID != null)
        {
            if (activeTypeDefGUIDs.get(typeGUID) != null)
            {
                return true;
            }

            if (activeAttributeTypeDefGUIDs.get(typeGUID) != null)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Return boolean indicating whether the (Attribute)TypeDef identifiers are valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeGUID - unique identifier of the TypeDef
     * @param typeName - unique name of the TypeDef
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String typeGUID,
                               String typeName)
    {
        if (typeName == null)
        {
            /*
             * A null TypeDef name is invalid
             */
            log.error("Null TypeDef Name from " + sourceName);

            return false;
        }

        if (typeGUID == null)
        {
            /*
             * A null guid is invalid
             */
            log.error("Null TypeDef GUID from " + sourceName);

            return false;
        }

        TypeDef typeDef = knownTypeDefNames.get(typeName);

        if (typeDef == null)
        {
            /*
             * This TypeDef is unknown so see if it is an AttributeTypeDef
             */
            AttributeTypeDef   attributeTypeDef = knownAttributeTypeDefNames.get(typeName);

            if (attributeTypeDef == null)
            {
                log.debug("Unknown (Attribute)TypeDef from " + sourceName);
            }
            else
            {
                if (!typeGUID.equals(attributeTypeDef.getGUID()))
                {
                    /*
                     * The requested guid does not equal the stored one.
                     */
                    log.error("GUID Mismatch in AttributeTypeDef " + typeName + " from " + sourceName + " received GUID is " + typeGUID + "; stored GUID is " + attributeTypeDef.getGUID());
                    return false;
                }

                log.debug("Valid AttributeTypeDef from " + sourceName);
                return true;
            }

            log.debug("Valid AttributeTypeDef from " + sourceName);
            return true;
        }

        if (! typeGUID.equals(typeDef.getGUID()))
        {
            /*
             * The requested guid does not equal the stored one.
             */
            log.error("GUID Mismatch in TypeDef " + typeName + " from " + sourceName + " received GUID is " + typeGUID + "; stored GUID is " + typeDef.getGUID());

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
    public boolean validTypeDefId(String          sourceName,
                                  String          typeDefGUID,
                                  String          typeDefName,
                                  TypeDefCategory category)
    {
        if (! validTypeId(sourceName, typeDefGUID, typeDefName))
        {
            return false;
        }

        TypeDef          typeDef = knownTypeDefNames.get(typeDefName);

        if (typeDef != null)
        {
            TypeDefCategory knownTypeDefCategory = typeDef.getCategory();

            if (knownTypeDefCategory == null)
            {
                log.error("Unknown TypeDef Category for " + typeDefName + " (GUID = " + typeDefGUID + ") from " + sourceName);
                return false;
            }

            if (category.getTypeCode() != knownTypeDefCategory.getTypeCode())
            {
                log.error("TypeDef category mismatch for TypeDef " + typeDefName + " (GUID = " + typeDefGUID + ") from "
                                  + sourceName + " received version number is " + category.getTypeDescription()
                                  + " and stored category is " + knownTypeDefCategory.getTypeDescription());

                return false;
            }
        }

        return true;
    }


    /**
     * Return boolean indicating whether the AttributeTypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier of the AttributeTypeDef
     * @param attributeTypeDefName - unique name of the AttributeTypeDef
     * @param category - category for the AttributeTypeDef
     * @return boolean result
     */
    public boolean validAttributeTypeDefId(String                   sourceName,
                                           String                   attributeTypeDefGUID,
                                           String                   attributeTypeDefName,
                                           AttributeTypeDefCategory category)
    {
        if (! validTypeId(sourceName, attributeTypeDefGUID, attributeTypeDefName))
        {
            return false;
        }

        AttributeTypeDef          attributeTypeDef = knownAttributeTypeDefNames.get(attributeTypeDefName);

        if (attributeTypeDef != null)
        {
            AttributeTypeDefCategory knownAttributeTypeDefCategory = attributeTypeDef.getCategory();

            if (knownAttributeTypeDefCategory == null)
            {
                log.error("Unknown AttributeTypeDef Category for " + attributeTypeDefName + " (GUID = " + attributeTypeDefGUID + ") from " + sourceName);
                return false;
            }

            if (category.getTypeCode() != knownAttributeTypeDefCategory.getTypeCode())
            {
                log.error("TypeDef category mismatch for TypeDef " + attributeTypeDefName + " (GUID = " + attributeTypeDefGUID + ") from "
                                  + sourceName + " received version number is " + category.getTypeDescription()
                                  + " and stored category is " + knownAttributeTypeDefCategory.getTypeDescription());

                return false;
            }
        }

        return true;
    }


    /**
     * Return boolean indicating whether the TypeDef identifiers are valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param typeDefVersion - version of the type
     * @param typeDefCategory - category of the instance described by this TypeDef.
     * @return boolean result
     */
    public boolean validTypeDefId(String          sourceName,
                                  String          typeDefGUID,
                                  String          typeDefName,
                                  long            typeDefVersion,
                                  TypeDefCategory typeDefCategory)
    {
        if (! validTypeDefId(sourceName, typeDefGUID, typeDefName, typeDefCategory))
        {
            return false;
        }

        TypeDef   typeDef = knownTypeDefNames.get(typeDefName);

        if (typeDef == null)
        {
            log.debug("Unknown TypeDef " + typeDefName + " (GUID = " + typeDefGUID + ") from " + sourceName);

            return true;
        }

        if (typeDef.getVersion() != typeDefVersion)
        {
            log.error("Version mismatch for TypeDef " + typeDefName + " (GUID = " + typeDefGUID + ") from "
                              + sourceName + " received version number is " + Long.toString(typeDefVersion)
                              + " and stored version is " + Long.toString(typeDef.getVersion()));

            return false;
        }

        return true;
    }


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier of the TypeDef
     * @param attributeTypeDefName - unique name of the TypeDef
     * @param attributeTypeDefVersion - version of the type
     * @param category - category for the TypeDef
     * @return boolean result
     */
    public boolean validAttributeTypeDefId(String                   sourceName,
                                           String                   attributeTypeDefGUID,
                                           String                   attributeTypeDefName,
                                           long                     attributeTypeDefVersion,
                                           AttributeTypeDefCategory category)
    {
        if (! validAttributeTypeDefId(sourceName, attributeTypeDefGUID, attributeTypeDefName, category))
        {
            return false;
        }

        TypeDef   typeDef = knownTypeDefNames.get(attributeTypeDefName);

        if (typeDef == null)
        {
            log.debug("Unknown TypeDef " + attributeTypeDefName + " (GUID = " + attributeTypeDefGUID + ") from " + sourceName);

            return true;
        }

        if (typeDef.getVersion() != attributeTypeDefVersion)
        {
            log.error("Version mismatch for TypeDef " + attributeTypeDefName + " (GUID = " + attributeTypeDefGUID + ") from "
                              + sourceName + " received version number is " + Long.toString(attributeTypeDefVersion)
                              + " and stored version is " + Long.toString(typeDef.getVersion()));

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
            log.error("Null typeDef from " + sourceName);
            return false;
        }

        if (validTypeDefId(sourceName,
                           typeDef.getGUID(),
                           typeDef.getName(),
                           typeDef.getVersion(),
                           typeDef.getCategory()))
        {
            log.debug("Good typeDef from " + sourceName);
            return true;
        }
        else
        {
            log.error("Bad typeDef from " + sourceName);
            return false;
        }
    }


    /**
     * Return boolean indicating whether the supplied AttributeTypeDef is valid or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDef - TypeDef to test
     * @return boolean result
     */
    public boolean validAttributeTypeDef(String           sourceName,
                                         AttributeTypeDef attributeTypeDef)
    {
        if (attributeTypeDef == null)
        {
            log.error("Null attributeTypeDef from " + sourceName);
            return false;
        }

        if (validAttributeTypeDefId(sourceName,
                                    attributeTypeDef.getGUID(),
                                    attributeTypeDef.getName(),
                                    attributeTypeDef.getCategory()))
        {
            log.debug("Good attributeTypeDef from " + sourceName);
            return true;
        }
        else
        {
            log.error("Bad attributeTypeDef from " + sourceName);
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
            if (validTypeDefId(sourceName,
                               typeDefSummary.getGUID(),
                               typeDefSummary.getName(),
                               typeDefSummary.getVersion(),
                               typeDefSummary.getCategory()))
            {
                return true;
            }
            else
            {
                log.error("Bad typeDefSummary from " + sourceName);
            }
        }

        log.error("Null typeDefSummary from " + sourceName);

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
        final String   actionDescription = "Process New TypeDef Event";

        OMRSMetadataCollection metadataCollection = null;

        try
        {
            if (localRepositoryConnector != null)
            {
                metadataCollection = localRepositoryConnector.getMetadataCollection();
            }

            if (metadataCollection != null)
            {
                /*
                 * VerifyTypeDef returns true if the typeDef is known and matches the supplied definition.
                 * It returns false if the type is supportable but has not yet been defined.
                 * It throws TypeDefNotSupportedException if the typeDef is not supported and can not
                 * be dynamically defined by the local repository.
                 */
                if (!metadataCollection.verifyTypeDef(sourceName, typeDef))
                {
                    metadataCollection.addTypeDef(sourceName, typeDef);

                    /*
                     * Update the active TypeDefs as this new TypeDef has been accepted by the local repository.
                     */
                    activeTypeDefNames.put(typeDef.getName(), typeDef);

                    OMRSAuditCode auditCode = OMRSAuditCode.NEW_TYPE_ADDED;
                    auditLog.logRecord(actionDescription,
                                       auditCode.getLogMessageId(),
                                       auditCode.getSeverity(),
                                       auditCode.getFormattedLogMessage(typeDef.getName(),
                                                                        typeDef.getGUID(),
                                                                        Long.toString(typeDef.getVersion()),
                                                                        sourceName),
                                       null,
                                       auditCode.getSystemAction(),
                                       auditCode.getUserAction());
                }
            }
        }
        catch (TypeDefNotSupportedException fixedTypeSystemResponse)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.NEW_TYPE_NOT_SUPPORTED;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(typeDef.getName(),
                                                                typeDef.getGUID(),
                                                                Long.toString(typeDef.getVersion())),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("TypeDef not added because repository does not support dynamic type definitions", typeDef);
                log.debug("TypeDefNotSupportedException:", fixedTypeSystemResponse);

            }
        }
        catch (RepositoryErrorException error)
        {
            log.error("TypeDef " + typeDef.getName() + " not added because repository is not available", typeDef);
            log.error("RepositoryErrorException:", error);
        }
        catch (TypeDefConflictException error)
        {
            // TODO log an error to say that the TypeDef conflicts with a TypeDef already stored.


            log.error("TypeDef not added because it conflicts with another TypeDef already in the repository", typeDef);
            log.error("TypeDefConflictException:", error);

            outboundRepositoryEventManager.processTypeDefConflictEvent(sourceName,
                                                                       localRepositoryConnector.getMetadataCollectionId(),
                                                                       localRepositoryConnector.getLocalServerName(),
                                                                       localRepositoryConnector.getLocalServerType(),
                                                                       localRepositoryConnector.getOrganizationName(),
                                                                       typeDef,
                                                                       originatorMetadataCollectionId,
                                                                       knownTypeDefNames.get(typeDef.getName()),
                                                                       null);
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
        final String   actionDescription = "Process New TypeDef Event";

        OMRSMetadataCollection metadataCollection = null;

        try
        {
            if (localRepositoryConnector != null)
            {
                metadataCollection = localRepositoryConnector.getMetadataCollection();
            }

            if (metadataCollection != null)
            {
                /*
                 * VerifyTypeDef returns true if the typeDef is known and matches the supplied definition.
                 * It returns false if the type is supportable but has not yet been defined.
                 * It throws TypeDefNotSupportedException if the typeDef is not supported and can not
                 * be dynamically defined by the local repository.
                 */
                if (!metadataCollection.verifyAttributeTypeDef(sourceName, attributeTypeDef))
                {
                    metadataCollection.addAttributeTypeDef(sourceName, attributeTypeDef);

                    /*
                     * Update the active TypeDefs as this new TypeDef has been accepted by the local repository.
                     */
                    activeAttributeTypeDefNames.put(attributeTypeDef.getName(), attributeTypeDef);

                    OMRSAuditCode auditCode = OMRSAuditCode.NEW_TYPE_ADDED;
                    auditLog.logRecord(actionDescription,
                                       auditCode.getLogMessageId(),
                                       auditCode.getSeverity(),
                                       auditCode.getFormattedLogMessage(attributeTypeDef.getName(),
                                                                        attributeTypeDef.getGUID(),
                                                                        Long.toString(attributeTypeDef.getVersion()),
                                                                        sourceName),
                                       null,
                                       auditCode.getSystemAction(),
                                       auditCode.getUserAction());
                }
            }
        }
        catch (TypeDefNotSupportedException fixedTypeSystemResponse)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.NEW_TYPE_NOT_SUPPORTED;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(attributeTypeDef.getName(),
                                                                attributeTypeDef.getGUID(),
                                                                Long.toString(attributeTypeDef.getVersion())),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            if (log.isDebugEnabled())
            {
                log.debug("TypeDef not added because repository does not support dynamic type definitions", attributeTypeDef);
                log.debug("TypeDefNotSupportedException:", fixedTypeSystemResponse);

            }
        }
        catch (RepositoryErrorException error)
        {
            log.error("TypeDef " + attributeTypeDef.getName() + " not added because repository is not available", attributeTypeDef);
            log.error("RepositoryErrorException:", error);
        }
        catch (TypeDefConflictException error)
        {
            // TODO log an error to say that the TypeDef conflicts with a TypeDef already stored.

            log.error("TypeDef not added because it conflicts with another TypeDef already in the repository", attributeTypeDef);
            log.error("TypeDefConflictException:", error);

            outboundRepositoryEventManager.processAttributeTypeDefConflictEvent(sourceName,
                                                                                localRepositoryConnector.getMetadataCollectionId(),
                                                                                localRepositoryConnector.getLocalServerName(),
                                                                                localRepositoryConnector.getLocalServerType(),
                                                                                localRepositoryConnector.getOrganizationName(),
                                                                                attributeTypeDef,
                                                                                originatorMetadataCollectionId,
                                                                                knownAttributeTypeDefNames.get(
                                                                                        attributeTypeDef.getName()),
                                                                                null);
        }
        catch (InvalidTypeDefException error)
        {
            // TODO log an error to say that the TypeDef contains bad values.

            if (log.isDebugEnabled())
            {
                log.debug("TypeDef not added because repository is not available", attributeTypeDef);
                log.debug("InvalidTypeDefException:", error);
            }
        }
        catch (TypeDefKnownException error)
        {
            // TODO log an error to say that a logic error has occurred

            if (log.isDebugEnabled())
            {
                log.debug("TypeDef not added because repository has a logic error", attributeTypeDef);
                log.debug("TypeDefKnownException:", error);

            }
        }
        catch (Throwable  error)
        {
            // TODO log an error to say that an unexpected error has occurred

            if (log.isDebugEnabled())
            {
                log.debug("TypeDef not added because repository has an unexpected error", attributeTypeDef);
                log.debug("Throwable:", error);
            }
        }
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
     * @param typeDefPatch - details of the new version of the TypeDef
     */
    public void processUpdatedTypeDefEvent(String       sourceName,
                                           String       originatorMetadataCollectionId,
                                           String       originatorServerName,
                                           String       originatorServerType,
                                           String       originatorOrganizationName,
                                           TypeDefPatch typeDefPatch)
    {
        try
        {
            OMRSMetadataCollection metadataCollection = localRepositoryConnector.getMetadataCollection();

            if (metadataCollection != null)
            {


                TypeDef updatedTypeDef = metadataCollection.updateTypeDef(null, typeDefPatch);

                if (log.isDebugEnabled())
                {
                    log.debug("Patch successfully applied", updatedTypeDef);
                }
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
     * A TypeDef from another member in the cohort is at a different version than the local repository.  This may
     * create some inconsistencies in the different copies of instances of this type in different members of the
     * cohort.  The recommended action is to update all TypeDefs to the latest version.
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


    /* ========================
     * Private error handling
     */


    /**
     * Throws a logic error exception when the repository content manager is called with invalid parameters.
     * Normally this means the repository content manager methods have been called in the wrong order.
     *
     * @param sourceName - source of the request (used for logging)
     * @param originatingMethodName - method that called the repository validator
     * @param localMethodName - local method that deleted the error
     */
    private void throwContentManagerLogicError(String     sourceName,
                                               String     originatingMethodName,
                                               String     localMethodName)
    {
        OMRSErrorCode errorCode = OMRSErrorCode.CONTENT_MANAGER_LOGIC_ERROR;
        String errorMessage     = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(sourceName,
                                                                                                     localMethodName,
                                                                                                     originatingMethodName);

        throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          localMethodName,
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
    }
}
