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
import org.apache.atlas.omrs.ffdc.exception.InvalidParameterException;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.PrimitiveDefCategory;

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * PrimitivePropertyValue stores a single primitive property.  This is stored in the specific Java class
 * for the property value's type although it is stored as an object.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class PrimitivePropertyValue extends InstancePropertyValue
{
    private  PrimitiveDefCategory   primitiveDefCategory = null;
    private  Object                 primitiveValue = null;


    /**
     * Default constructor sets the primitive property value to null.
     */
    public PrimitivePropertyValue()
    {
        super(InstancePropertyCategory.PRIMITIVE);
    }


    /**
     * Copy/clone constructor - copies the values from the supplied template.
     *
     * @param template - PrimitivePropertyValue
     */
    public PrimitivePropertyValue(PrimitivePropertyValue   template)
    {
        super(template);

        if (template != null)
        {
            this.primitiveDefCategory = template.getPrimitiveDefCategory();
            this.primitiveValue = template.getPrimitiveValue();
        }
    }


    /**
     * Return the the category of the primitive's type.  This sets the name and Java Class used for
     * the primitive value.
     *
     * @return PrimitiveDefCategory
     */
    public PrimitiveDefCategory getPrimitiveDefCategory() { return primitiveDefCategory; }


    /**
     * Set up the category of the primitive type.  This sets the name and Java Class used for
     * the primitive value.
     *
     * @param primitiveDefCategory - PrimitiveDefCategory enum
     */
    public void setPrimitiveDefCategory(PrimitiveDefCategory primitiveDefCategory)
    {
        /*
         * Tests that type and value are consistent
         */
        validateValueAgainstType(primitiveDefCategory, primitiveValue);

        /*
         * All ok so set the category
         */
        this.primitiveDefCategory = primitiveDefCategory;
    }


    /**
     * Return the primitive value.  It is already set up to be the appropriate type for the primitive
     * as defined in the PrimitiveDefCategory.
     *
     * @return Object containing the primitive value.
     */
    public Object getPrimitiveValue() { return primitiveValue; }


    /**
     * Set up the primitive value.   Although it is passed in as a java.lang.Object, it should be the correct
     * type as defined by the PrimitiveDefCategory.
     *
     * @param primitiveValue - object contain the primitive value
     */
    public void setPrimitiveValue(Object primitiveValue)
    {
        /*
         * Tests that type and value are consistent
         */
        validateValueAgainstType(primitiveDefCategory, primitiveValue);

        /*
         * The primitive value is of the correct type so save it.
         */
        this.primitiveValue = primitiveValue;
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "PrimitivePropertyValue{" +
                "primitiveDefCategory=" + primitiveDefCategory +
                ", primitiveValue=" + primitiveValue +
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
        PrimitivePropertyValue that = (PrimitivePropertyValue) o;
        return primitiveDefCategory == that.primitiveDefCategory &&
                Objects.equals(primitiveValue, that.primitiveValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(primitiveDefCategory, primitiveValue);
    }


    /**
     * Ensure that the type and value supplied are compatible.
     *
     * @param primitiveDefCategory - category to test
     * @param primitiveValue - value to test
     */
    private void validateValueAgainstType(PrimitiveDefCategory   primitiveDefCategory,
                                          Object                 primitiveValue)
    {
        final String  methodName = "setPrimitiveValue()";

        /*
         * Return if one of the values is missing
         */
        if ((primitiveDefCategory == null) || (primitiveValue == null))
        {
            return;
        }

        try
        {
            Class    testJavaClass = Class.forName(primitiveDefCategory.getJavaClassName());

            if (!testJavaClass.isInstance(primitiveValue))
            {
                /*
                 * The primitive value supplied is the wrong type.  Throw an exception.
                 */
                OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_PRIMITIVE_VALUE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(primitiveDefCategory.getJavaClassName(),
                                                                                primitiveDefCategory.getName());

                throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  methodName,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction());
            }
        }
        catch (ClassNotFoundException    unknownPrimitiveClass)
        {
            /*
             * The java class defined in the primitiveDefCategory is not known.  This is an internal error
             * that needs a code fix in PrimitiveDefCategory.
             */
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_PRIMITIVE_CLASS_NAME;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(primitiveDefCategory.getJavaClassName(),
                                                         primitiveDefCategory.getName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              unknownPrimitiveClass);
        }
        catch (Error    invalidPrimitiveCategory)
        {
            /*
             * Some unexpected exception occurred when manipulating the Java Classes.  Probably a coding error.
             */
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_PRIMITIVE_CATEGORY;
            String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(primitiveDefCategory.getName());

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction(),
                                              invalidPrimitiveCategory);
        }
    }
}
