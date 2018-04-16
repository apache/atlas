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
package org.apache.atlas.omas.connectedasset.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omas.connectedasset.ffdc.ConnectedAssetErrorCode;
import org.apache.atlas.omas.connectedasset.ffdc.exceptions.ConnectedAssetRuntimeException;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The Classification class stores information about a classification assigned to an asset.  The Classification
 * has a name and some properties.  It also stores the typename of the asset it is connected to for debug purposes.
 *
 * Note: it is not valid to have a classification with a null or blank name.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Classification extends PropertyBase
{
    private String                       classificationName;
    private AdditionalProperties         classificationProperties;

    /**
     * A private validation method used by the constructors.
     *
     * @param name - name to check
     * @return validated name
     */
    private String validateName(String   name)
    {
        final String  methodName = "validateName";

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
            ConnectedAssetErrorCode errorCode = ConnectedAssetErrorCode.NULL_CLASSIFICATION_NAME;
            String       errorMessage = errorCode.getErrorMessageId()
                                      + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName());

            throw new ConnectedAssetRuntimeException(errorCode.getHTTPErrorCode(),
                                                     this.getClass().getName(),
                                                     methodName,
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
     */
    public Classification(String               name,
                          AdditionalProperties properties)
    {
        super();

        this.classificationName = validateName(name);
        this.classificationProperties = properties;
    }


    /**
     * Copy/clone Constructor - sets up new classification using values from the template
     *
     * @param templateClassification - object to copy
     */
    public Classification(Classification templateClassification)
    {
        super(templateClassification);

        final String  methodName = "Constructor";

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
            ConnectedAssetErrorCode errorCode = ConnectedAssetErrorCode.NULL_CLASSIFICATION_NAME;
            String       errorMessage = errorCode.getErrorMessageId()
                                      + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName());

            throw new ConnectedAssetRuntimeException(errorCode.getHTTPErrorCode(),
                                                     this.getClass().getName(),
                                                     methodName,
                                                     errorMessage,
                                                     errorCode.getSystemAction(),
                                                     errorCode.getUserAction());
        }
        else
        {
            /*
             * Save the name and properties.
             */
            this.classificationName = validateName(templateClassification.getName());
            this.classificationProperties = templateClassification.getProperties();
        }
    }


    /**
     * Return the name of the classification
     *
     * @return name of classification
     */
    public String getName()
    {
        return classificationName;
    }


    /**
     * Returns a collection of the additional stored properties for the classification.
     * If no stored properties are present then null is returned.
     *
     * @return properties for the classification
     */
    public AdditionalProperties getProperties()
    {
        if (classificationProperties == null)
        {
            return classificationProperties;
        }
        else
        {
            return new AdditionalProperties(classificationProperties);
        }
    }
}