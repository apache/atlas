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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;

/**
 * TypeVisitor implementation that builds up a list of type vertices
 * that need to be created for the types that are being stored.
 *
 */
public class TypeVertexFinder implements TypeVisitor {

    private final List<TypeVertexInfo> toCreate = new ArrayList<TypeVertexInfo>();
    private final Set<String> typesIncluded = new HashSet<String>();
    private final TypeSystem typeSystem;

    public TypeVertexFinder(TypeSystem ts) {
        typeSystem = ts;
    }


    @Override
    public void visitEnumeration(EnumType dataType) {
        visitDataType(dataType);
    }

    private void addTypeIfNeeded(TypeVertexInfo info) {
        if(! typesIncluded.contains(info.getTypeName())) {
            toCreate.add(info);
            typesIncluded.add(info.getTypeName());
        }
    }

    @Override
    public void visitAttributeDataType(String typeName, AttributeInfo sourceAttr, IDataType attrType) throws AtlasException {
        visitDataType(attrType);
    }

    @Override
    public void visitSuperType(String typeName, String superTypeName) throws AtlasException {
        HierarchicalType superType = typeSystem.getDataType(HierarchicalType.class, superTypeName);
        visitDataType(superType);
    }

    @Override
    public void visitAttributeNames(String typeName, List<String> attrNames) throws AtlasException {
       //nothing to do

    }

    @Override
    public void visitAttribute(String typeName, AttributeInfo attribute) throws StorageException, AtlasException {
        //nothing to do
    }


    private void visitDataType(IDataType dataType) {
        TypeVertexInfo info = null;
        info = new TypeVertexInfo(dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
        addTypeIfNeeded(info);

    }


    public List<TypeVertexInfo> getVerticesToCreate() {
        return toCreate;
    }

    @Override
    public void visitDataType(TypeCategory category, String typeName, String typeDescription) {
        TypeVertexInfo info = new TypeVertexInfo(category, typeName, typeDescription);
        addTypeIfNeeded(info);
    }

}