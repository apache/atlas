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

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * ArrayPropertyValue stores the values of an array within an entity, struct or relationship properties.
 * The elements of the array are stored in an InstanceProperties map where the property name is set to the element
 * number and the property value is set to the value of the element in the array.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class ArrayPropertyValue extends InstancePropertyValue
{
    private  int                   arrayCount = 0;
    private  InstanceProperties    arrayValues = null;


    /**
     * Default constructor sets the array to empty.
     */
    public ArrayPropertyValue()
    {
        super(InstancePropertyCategory.ARRAY);
    }


    /**
     * Copy/clone constructor set up the array using the supplied template.
     *
     * @param template - ArrayPropertyValue
     */
    public ArrayPropertyValue(ArrayPropertyValue   template)
    {
        super(template);

        if (template !=null)
        {
            arrayCount = template.getArrayCount();
            arrayValues = template.getArrayValues();
        }
    }


    /**
     * Return the number of elements in the array.
     *
     * @return int - array size
     */
    public int getArrayCount() { return arrayCount; }


    /**
     * Set up the number of elements in the array.
     *
     * @param arrayCount - int - array size
     */
    public void setArrayCount(int arrayCount) { this.arrayCount = arrayCount; }


    /**
     * Return a copy of the array elements.
     *
     * @return InstanceProperties containing the array elements
     */
    public InstanceProperties getArrayValues()
    {
        if (arrayValues == null)
        {
            return arrayValues;
        }
        else
        {
            return new InstanceProperties(arrayValues);
        }
    }


    /**
     * Add or update an element in the array.
     *
     * @param elementNumber - index number of the element in the array
     * @param propertyValue - value to store
     */
    public void setArrayValue(int  elementNumber, InstancePropertyValue  propertyValue)
    {
        if (arrayCount > elementNumber)
        {
            if (arrayValues == null)
            {
                arrayValues = new InstanceProperties();
            }
            arrayValues.setProperty(new Integer(elementNumber).toString(), propertyValue);
        }
        else
        {
            /*
             * Throw runtime exception to show the caller they are not using the array correctly.
             */
            OMRSErrorCode errorCode    = OMRSErrorCode.ARRAY_OUT_OF_BOUNDS;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(this.getClass().getSimpleName(),
                                                                            new Integer(elementNumber).toString(),
                                                                            new Integer(arrayCount).toString());

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           "setArrayValue",
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction());
        }
    }


    /**
     * Set up the array elements in one call.
     *
     * @param arrayValues - InstanceProperties containing the array elements
     */
    public void setArrayValues(InstanceProperties arrayValues) { this.arrayValues = arrayValues; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "ArrayPropertyValue{" +
                "arrayCount=" + arrayCount +
                ", arrayValues=" + arrayValues +
                ", instancePropertyCategory=" + getInstancePropertyCategory() +
                ", typeGUID='" + getTypeGUID() + '\'' +
                ", typeName='" + getTypeName() + '\'' +
                '}';
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ArrayPropertyValue that = (ArrayPropertyValue) o;
        return arrayCount == that.arrayCount &&
                Objects.equals(arrayValues, that.arrayValues);
    }


    @Override
    public int hashCode()
    {

        return Objects.hash(arrayCount, arrayValues);
    }
}
