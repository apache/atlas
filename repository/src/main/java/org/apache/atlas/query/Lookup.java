/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.query;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.type.AtlasType;

public interface Lookup {
    AtlasType getType(String typeName) throws AtlasBaseException;

    String getQualifiedName(GremlinQueryComposer.Context context, String name) throws AtlasBaseException;

    boolean isPrimitive(GremlinQueryComposer.Context context, String attributeName);

    String getRelationshipEdgeLabel(GremlinQueryComposer.Context context, String attributeName);

    AtlasRelationshipEdgeDirection getRelationshipEdgeDirection(GremlinQueryComposer.Context context, String attributeName);

    boolean hasAttribute(GremlinQueryComposer.Context context, String typeName);

    boolean doesTypeHaveSubTypes(GremlinQueryComposer.Context context);

    String getTypeAndSubTypes(GremlinQueryComposer.Context context);

    boolean isTraitType(String s);

    String getTypeFromEdge(GremlinQueryComposer.Context context, String item);

    boolean isDate(GremlinQueryComposer.Context context, String attributeName);

    boolean isNumeric(GremlinQueryComposer.Context context, String attrName);

    String getVertexPropertyName(String typeName, String attrName);
}
