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

package org.apache.atlas.query;

import org.apache.atlas.type.AtlasType;

public interface Lookup {
    AtlasType getType(String typeName);

    String getQualifiedName(QueryProcessor.Context context, String name);

    boolean isPrimitive(QueryProcessor.Context context, String attributeName);

    String getRelationshipEdgeLabel(QueryProcessor.Context context, String attributeName);

    boolean hasAttribute(QueryProcessor.Context context, String typeName);

    boolean doesTypeHaveSubTypes(QueryProcessor.Context context);

    String getTypeAndSubTypes(QueryProcessor.Context context);

    boolean isTraitType(QueryProcessor.Context context);

    String getTypeFromEdge(QueryProcessor.Context context, String item);

    boolean isDate(QueryProcessor.Context context, String attributeName);
}
