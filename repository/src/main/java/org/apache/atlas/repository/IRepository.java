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

package org.apache.atlas.repository;

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.TraitType;

import java.util.List;

/**
 * Metadata Repository interface.
 */
@Deprecated
public interface IRepository {

    ITypedReferenceableInstance create(IReferenceableInstance i) throws RepositoryException;

    ITypedReferenceableInstance update(ITypedReferenceableInstance i) throws RepositoryException;

    void delete(ITypedReferenceableInstance i) throws RepositoryException;

    Id newId(String typeName);

    ITypedReferenceableInstance get(Id id) throws RepositoryException;

    void defineClass(ClassType type) throws RepositoryException;

    void defineTrait(TraitType type) throws RepositoryException;

    void defineTypes(List<HierarchicalType> types) throws RepositoryException;
}
