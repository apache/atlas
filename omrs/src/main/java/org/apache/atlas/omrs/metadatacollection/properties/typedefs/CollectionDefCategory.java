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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.atlas.omrs.metadatacollection.properties.instances.ArrayPropertyValue;
import org.apache.atlas.omrs.metadatacollection.properties.instances.MapPropertyValue;
import org.apache.atlas.omrs.metadatacollection.properties.instances.StructPropertyValue;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * This enum defines the list of open metadata collection types.  These types are generic types that need to
 * be configured with specific primitive types before they can be used as an attribute type.
 *
 * The enum includes a code value, a string name for the type (used in self describing structures such as JSON or XML)
 * and the name of the Java Class that supports this type.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum CollectionDefCategory implements Serializable
{
    OM_COLLECTION_UNKNOWN (0, "<>",              0, null),
    OM_COLLECTION_MAP     (1, "map<{$0}, {$1}>", 2, MapPropertyValue.class.getName()),
    OM_COLLECTION_ARRAY   (2, "array<{$0}>",     1, ArrayPropertyValue.class.getName()),
    OM_COLLECTION_STRUCT  (3, "struct<>",        0, StructPropertyValue.class.getName());

    private static final long serialVersionUID = 1L;

    private  int         code;
    private  String      name;
    private  int         argumentCount;
    private  String      javaClassName;


    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param code - code for the enum
     * @param name - String name for the enum before it is configured with primitive types
     * @param argumentCount - number of arguments needed to configure the collection type
     * @param javaClassName - Java class used to manage this type of collection
     */
    CollectionDefCategory(int   code, String name, int argumentCount, String javaClassName)
    {
        this.code = code;
        this.name = name;
        this.argumentCount = argumentCount;
        this.javaClassName = javaClassName;
    }


    /**
     * Return the numeric code for the primitive type which can be used in optimized data flows.
     *
     * @return int type code
     */
    public int getCode() {
        return code;
    }


    /**
     * Return the name of type - which can be used for text-based interchange formats such as JSON or XML.
     *
     * @return String type name
     */
    public String getName() {
        return name;
    }


    /**
     * Return the number of arguments for this collection type.
     *
     * @return int number of elements
     */
    public int getArgumentCount() { return argumentCount; }


    /**
     * Return the name of the java class that can be used to store properties of this type.
     *
     * @return String java class name.
     */
    public String getJavaClassName() {
        return javaClassName;
    }
}
