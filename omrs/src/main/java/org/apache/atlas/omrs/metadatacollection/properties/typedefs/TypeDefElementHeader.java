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

import java.io.Serializable;

/**
 * TypeDefElementHeader provides a common base for all typedef information.
 */
public class TypeDefElementHeader implements Serializable
{
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor sets TypeDef to nulls.
     */
    public TypeDefElementHeader()
    {
        /*
         * Nothing to do
         */
    }


    /**
     * Copy/clone constructor set TypeDef to value in template.
     *
     * @param template - TypeDefElementHeader
     */
    public TypeDefElementHeader(TypeDefElementHeader  template)
    {
        /*
         * Nothing to do
         */
    }
}
