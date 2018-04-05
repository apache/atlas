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
package org.apache.atlas.omrs.metadatacollection.properties.instances;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * The Classification class stores information about a classification assigned to an entity.  The Classification
 * has a name and some properties.   Some classifications are explicitly added to an entity and other
 * classifications are propagated to an entity along the relationships connected to it.  The origin of the
 * classification is also stored.
 *
 * Note: it is not valid to have a classification with a null or blank name.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Classification extends InstanceAuditHeader
{
    private String               classificationName       = null;
    private InstanceProperties   classificationProperties = null;
    private ClassificationOrigin classificationOrigin     = null;
    private String               classificationOriginGUID = null;

    /*
     * A private validation method used by the constructors
     */
    private String validateName(String   name)
    {
        /*
         * Throw an exception if the classification's name is null because that does not make sense.
         * The constructors do not catch this exception so it is received by the creator of the classification
         * object.
         */
        if (name == null || name.equals(""))
        {
            /*
             * Build and throw exception.  This should not happen - likely to be a problem in the
             * repository connector.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_PROPERTY_NAME;
            String       errorMessage = errorCode.getErrorMessageId()
                                      + errorCode.getFormattedErrorMessage();

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "validateName",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }
        else
        {
            return name;
        }
    }


    /**
     * Typical constructor - verifies and saves parameters.
     *
     * @param name - name of the classification
     * @param properties - additional properties for the classification
     * @param origin - the origin of the classification
     * @param originGUID - the guid of the entity of the classification origin was propagated.
     */
    public Classification(String               name,
                          InstanceProperties   properties,
                          ClassificationOrigin origin,
                          String               originGUID)
    {
        this.classificationName = validateName(name);
        this.classificationProperties = properties;
        this.classificationOrigin = origin;
        this.classificationOriginGUID = originGUID;
    }


    /**
     * Default constructor for automated generation tools.
     */
    public Classification()
    {

    }


    /**
     * Copy/clone Constructor - sets up new classification using values from the template
     *
     * @param templateClassification - object to copy
     */
    public Classification(Classification templateClassification)
    {
        /*
         * An empty classification object is passed in the variable declaration so throw exception
         * because we need the classification name.
         */
        if (templateClassification == null)
        {
            /*
             * Build and throw exception.  This should not happen - likely to be a problem in the
             * repository connector.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_PROPERTY_NAME;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage("<Unknown>");

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           "Copy Constructor",
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction());
        }
        else
        {
            /*
             * Extract and save the values from the template.
             */
            this.classificationName = validateName(templateClassification.getName());
            this.classificationProperties = templateClassification.getProperties();
            this.classificationOrigin = templateClassification.getClassificationOrigin();
            this.classificationOriginGUID = templateClassification.getClassificationOriginGUID();
        }
    }


    /**
     * Return the name of the classification.
     *
     * @return name of classification
     */
    public String getName()
    {
        return classificationName;
    }


    /**
     * Set up the name of the classification.
     *
     * @param classificationName - String name
     */
    public void setName(String classificationName)
    {
        this.classificationName = validateName(classificationName);
    }


    /**
     * Returns a collection of the additional stored properties for the classification.
     * If no stored properties are present then null is returned.
     *
     * @return properties for the classification
     */
    public InstanceProperties getProperties()
    {
        if (classificationProperties == null)
        {
            return classificationProperties;
        }
        else
        {
            return new InstanceProperties(classificationProperties);
        }
    }


    /**
     * Set up a collection of the additional stored properties for the classification.
     *
     * @param classificationProperties - properties object
     */
    public void setProperties(InstanceProperties classificationProperties)
    {
        this.classificationProperties = classificationProperties;
    }


    /**
     * Return the origin of the classification.
     *
     * @return ClassificationOrigin enum
     */
    public ClassificationOrigin getClassificationOrigin()
    {
        return classificationOrigin;
    }


    /**
     * Set up the origin of the classification.
     *
     * @param classificationOrigin - ClassificationOrigin enum
     */
    public void setClassificationOrigin(ClassificationOrigin classificationOrigin)
    {
        this.classificationOrigin = classificationOrigin;
    }


    /**
     * Return the guid of the entity where a propagate classification came from.
     *
     * @return unique identifier of the classification's origin
     */
    public String getClassificationOriginGUID()
    {
        return classificationOriginGUID;
    }


    /**
     * Set up the guid of the entity where a propagate classification came from.
     *
     * @param classificationOriginGUID - unique identifier of the classification's origin
     */
    public void setClassificationOriginGUID(String classificationOriginGUID)
    {
        this.classificationOriginGUID = classificationOriginGUID;
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "Classification{" +
                "classificationName='" + classificationName + '\'' +
                ", classificationProperties=" + classificationProperties +
                ", classificationOrigin=" + classificationOrigin +
                ", classificationOriginGUID='" + classificationOriginGUID + '\'' +
                '}';
    }
}
