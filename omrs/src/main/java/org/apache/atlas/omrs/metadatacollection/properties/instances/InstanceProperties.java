
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * The InstanceProperties class provides support for arbitrary properties to be added to an entity,
 * struct or relationship object.
 * It wraps a java.util.Map map object built around HashMap.  The property name (or domain) of the map is the name
 * of the property.  The property value (or range) of the map is a subclass of InstancePropertyValue depending on
 * the type of the property:
 * <ul>
 *     <li>
 *         PrimitivePropertyValue - for primitives such as strings and numbers.  The full list of primitives are
 *         given in PrimitiveDefCategory.
 *     </li>
 *     <li>
 *         EnumPropertyValue - for properties with a type consisting of an enumeration of valid values.  Each
 *     </li>
 *     <li>
 *         StructPropertyValue - for properties that have a type of a complex structure (aka struct).
 *         The Struct can be thought of as a list of related properties.
 *     </li>
 *     <li>
 *         MapPropertyValue - for properties that have a type of map.
 *         The map holds an unordered list of name-value pairs.  The pairs are of the same type and the name for
 *         the pair is unique within the map.
 *     </li>
 *     <li>
 *         ArrayPropertyValue - for properties that have a type of Array.
 *         This is an ordered list of values of the same type.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class InstanceProperties extends InstanceElementHeader
{
    /*
     * Map from property name to property value.  The value includes type information.
     */
    private Map<String, InstancePropertyValue>  instanceProperties = new HashMap<>();


    /**
     * Typical constructor
     */
    public InstanceProperties()
    {
        super();
    }


    /**
     * Copy/clone Constructor.
     *
     * @param templateProperties - template object to copy.
     */
    public InstanceProperties(InstanceProperties templateProperties)
    {
        super(templateProperties);

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
                    String                 newPropertyName  = propertyNames.next();
                    InstancePropertyValue  newPropertyValue = templateProperties.getPropertyValue(newPropertyName);

                    instanceProperties.put(newPropertyName, newPropertyValue);
                }
            }
        }
    }


    /**
     * Return the instance properties as a map.
     *
     * @return  instance properties map.
     */
    public Map<String, InstancePropertyValue> getInstanceProperties()
    {
        return instanceProperties;
    }


    /**
     * Set up the instance properties map.
     *
     * @param instanceProperties - map of name valued properties
     */
    public void setInstanceProperties(Map<String, InstancePropertyValue> instanceProperties)
    {
        this.instanceProperties = instanceProperties;
    }

    /**
     * Returns a list of the instance properties for the element.
     * If no stored properties are present then null is returned.
     *
     * @return list of properties
     */
    public Iterator<String> getPropertyNames()
    {
        return instanceProperties.keySet().iterator();
    }


    /**
     * Returns the requested instance property for the element.
     * If no stored property with that name is present then null is returned.
     *
     * @param name - String name of the property to return.
     * @return requested property value.
     */
    public InstancePropertyValue getPropertyValue(String name)
    {
        return instanceProperties.get(name);
    }


    /**
     * Adds or updates an instance property.
     * If a null is supplied for the property name, an OMRS runtime exception is thrown.
     * If a null is supplied for the property value, the property is removed.
     *
     * @param  newPropertyName - name
     * @param  newPropertyValue - value
     */
    public void setProperty(String newPropertyName, InstancePropertyValue newPropertyValue)
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
            instanceProperties.remove(newPropertyName);
        }
        else
        {
            instanceProperties.put(newPropertyName, newPropertyValue);
        }
    }


    /**
     * Return the number of properties stored.
     *
     * @return int property count
     */
    public int getPropertyCount()
    {
        return instanceProperties.size();
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "InstanceProperties{" +
                "instanceProperties=" + instanceProperties +
                ", propertyNames=" + getPropertyNames() +
                ", propertyCount=" + getPropertyCount() +
                '}';
    }
}

