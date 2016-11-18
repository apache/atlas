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

package org.apache.atlas.typesystem;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.typesystem.persistence.AtlasSystemAttributes;
import org.apache.atlas.typesystem.persistence.Id;

/**
 * Represents and instance of a ClassType. These have identity.
 * Transient instances will have a UNASSIGNED identity.
 */
public interface IReferenceableInstance extends IStruct {

    ImmutableList<String> getTraits();

    Id getId();

    IStruct getTrait(String typeName);

    AtlasSystemAttributes getSystemAttributes();
}
