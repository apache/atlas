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
package org.apache.atlas.ocf.properties;

import java.io.Serializable;
import java.util.UUID;


/**
 * This property header implements any common mechanisms that all property objects need.
 */
public abstract class PropertyBase implements Serializable
{
    private static final long     serialVersionUID = 1L;
    private              int      hashCode = UUID.randomUUID().hashCode();


    /**
     * Typical Constructor
     */
    public PropertyBase()
    {
        /*
         * Nothing to do.  This constructor is included so variables are added in this class at a later date
         * without impacting the subclasses.
         */
    }


    /**
     * Copy/clone Constructor - the resulting object will return true if tested with this.equals(template) as
     * long as the template object is not null;
     *
     * @param template - object being copied
     */
    public PropertyBase(PropertyBase template)
    {
        /*
         * The hashCode value is replaced with the value from the template so the template object and this
         * new object will return equals set to true.
         */
        if (template != null)
        {
            hashCode = template.hashCode();
        }
    }


    /**
     * Provide a common implementation of hashCode for all OCF properties objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to describe the hash code of the properties
     * object.  This method may be overridden by subclasses.
     */
    public int hashCode()
    {
        return hashCode;
    }


    /**
     * Provide a common implementation of equals for all OCF properties objects.  The UUID is unique and
     * is randomly assigned and so its hashCode is as good as anything to evaluate the equality of the properties
     * object.
     *
     * @param object - object to test
     * @return boolean flag
     */
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        PropertyBase that = (PropertyBase) object;

        return hashCode == that.hashCode;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "PropertyBase{" +
                "hashCode=" + hashCode +
                '}';
    }
}