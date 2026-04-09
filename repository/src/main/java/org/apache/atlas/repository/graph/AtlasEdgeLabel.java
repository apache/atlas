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
package org.apache.atlas.repository.graph;

/**
 * Represents an edge label used in Atlas.
 * The format of an Atlas edge label is EDGE_LABEL_PREFIX<<typeName>>.<<attributeName>>[.mapKey]
 *
 */
public class AtlasEdgeLabel {
    private final String typeName;
    private final String attributeName;
    private final String mapKey;
    private final String edgeLabel;
    private final String qualifiedMapKey;
    private final String qualifiedAttributeName;

    public AtlasEdgeLabel(String edgeLabel) {
        String   labelWithoutPrefix = edgeLabel.substring(GraphHelper.EDGE_LABEL_PREFIX.length());
        String[] fields             = labelWithoutPrefix.split("\\.", 3);

        if (fields.length < 2 || fields.length > 3) {
            throw new IllegalArgumentException("Invalid edge label " + edgeLabel + ": expected 2 or 3 label components but found " + fields.length);
        }

        typeName      = fields[0];
        attributeName = fields[1];

        if (fields.length == 3) {
            mapKey                 = fields[2];
            qualifiedMapKey        = labelWithoutPrefix;
            qualifiedAttributeName = typeName + '.' + attributeName;
        } else {
            mapKey          = null;
            qualifiedMapKey = null;
            qualifiedAttributeName = labelWithoutPrefix;
        }

        this.edgeLabel = edgeLabel;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getMapKey() {
        return mapKey;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public String getQualifiedMapKey() {
        return qualifiedMapKey;
    }

    public String getQualifiedAttributeName() {
        return qualifiedAttributeName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append('(').append("typeName: ").append(typeName);
        sb.append(", attributeName: ").append(attributeName);

        if (mapKey != null) {
            sb.append(", mapKey: ").append(mapKey);
            sb.append(", qualifiedMapKey: ").append(qualifiedMapKey);
        }
        sb.append(", edgeLabel: ").append(edgeLabel).append(')');

        return sb.toString();
    }
}
