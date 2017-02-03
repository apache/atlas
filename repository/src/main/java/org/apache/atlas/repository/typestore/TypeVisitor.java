/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.typestore;

import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.IDataType;

/**
 * Callback mechanism used when storing types.  As {@link GraphBackedTypeStore} traverses
 * through the types being persisted, these methods are called with the information that
 * it finds.
 */
public interface TypeVisitor {

    /**
     * Called when an enumeration type is found
     * @param type
     * @throws AtlasException
     */
    void visitEnumeration(EnumType type) throws AtlasException;

    /**
     * Called with a data type that is associated with a given attribute.  There can
     * be more than one.  For example, map types have both a key and a value type.
     * This is called once for each type.  This is called once for each datatype
     * associated with the given attribute.
     *
     * @param typeName The name of the type being processed.
     * @param sourceAttr The attribute in that type that we are processing.
     * @param attrType A dataType associated with that attribute.
     * @throws AtlasException
     */
    void visitAttributeDataType(String typeName, AttributeInfo sourceAttr, IDataType attrType) throws AtlasException;

    /**
     * Called when a super type is found.  It is called once for each superType.
     *
     * @param typeName The type being processed.
     * @param superType The name of the super type that was found.
     * @throws RepositoryException
     * @throws AtlasException
     */
    void visitSuperType(String typeName, String superType) throws RepositoryException, AtlasException;

    /**
     * Called with the list of immediate attribute names that were found for the given type.  It
     * is called once per type.
     *
     * @param typeName The name of the type that is being processed.
     * @param attrNames The names of all of the immediate attributes in the type.
     * @throws AtlasException
     */
    void visitAttributeNames(String typeName, List<String> attrNames) throws AtlasException;

    /**
     * Called once for each immediate attribute in a type.
     * @param typeName The name of the type that is being procesed
     * @param attribute The immediate attribute that was found
     *
     * @throws StorageException
     * @throws AtlasException
     */
    void visitAttribute(String typeName, AttributeInfo attribute) throws StorageException, AtlasException;

    /**
     * Called once for each struct, class, and trait type that was found.  It is
     * called when we start processing that type.
     *
     * @param category The category of the type
     * @param typeName The name of the type
     * @param typeDescription The description of the type.
     */
    void visitDataType(TypeCategory category, String typeName, String typeDescription);
}