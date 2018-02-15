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
package org.apache.atlas.omrs.metadatacollection.properties.typedefs;

import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * The TypeDefProperties class provides support for arbitrary properties that belong to a TypeDef object.
 * It is used for searching the TypeDefs.
 * It wraps a java.util.Map map object built around HashMap.
 */
public class TypeDefProperties extends TypeDefElementHeader
{
    private Map<String,Object>  typeDefProperties = new HashMap<>();


    /**
     * Typical constructor
     */
    public TypeDefProperties()
    {
        /*
         * Nothing to do
         */
    }


    /**
     * Copy/clone Constructor.
     *
     * @param templateProperties - template object to copy.
     */
    public TypeDefProperties(TypeDefProperties templateProperties)
    {
        /*
         * An empty properties object is created in the private variable declaration so nothing to do.
         */
        if (templateProperties != null)
        {
            /*
             * Process templateProperties if they are not null
             */
            Iterator<String> propertyNames = templateProperties.getPropertyNames();

            if (propertyNames != null)
            {
                while (propertyNames.hasNext())
                {
                    String newPropertyName = propertyNames.next();
                    Object newPropertyValue = templateProperties.getProperty(newPropertyName);

                    typeDefProperties.put(newPropertyName, newPropertyValue);
                }
            }
        }
    }


    /**
     * Returns a list of the instance properties for the element.
     * If no stored properties are present then null is returned.
     *
     * @return list of properties
     */
    public Iterator<String> getPropertyNames()
    {
        return typeDefProperties.keySet().iterator();
    }


    /**
     * Returns the requested instance property for the element.
     * If no stored property with that name is present then null is returned.
     *
     * @param name - String name of the property to return.
     * @return requested property value.
     */
    public Object getProperty(String name)
    {
        return typeDefProperties.get(name);
    }


    /**
     * Adds or updates an instance property.
     * If a null is supplied for the property name, an OMRS runtime exception is thrown.
     * If a null is supplied for the property value, the property is removed.
     *
     * @param  newPropertyName - name
     * @param  newPropertyValue - value
     */
    public void setProperty(String newPropertyName, Object newPropertyValue)
    {
        if (newPropertyName == null)
        {
            /*
             * Build and throw exception.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_PROPERTY_NAME;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage();

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                          this.getClass().getName(),
                                          "setProperty",
                                          errorMessage,
                                          errorCode.getSystemAction(),
                                          errorCode.getUserAction());
        }
        else if (newPropertyValue == null)
        {
            typeDefProperties.remove(newPropertyName);
        }
        else
        {
            typeDefProperties.put(newPropertyName, newPropertyValue);
        }
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "TypeDefProperties{" +
                "typeDefProperties=" + typeDefProperties +
                '}';
    }
}
