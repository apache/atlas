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
package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Deprecated
public class FullTextMapper {

    private static final Logger LOG = LoggerFactory.getLogger(FullTextMapper.class);

    private final GraphToTypedInstanceMapper graphToTypedInstanceMapper;
    private final TypedInstanceToGraphMapper typedInstanceToGraphMapper;

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private static final String FULL_TEXT_DELIMITER = " ";

    public FullTextMapper(TypedInstanceToGraphMapper typedInstanceToGraphMapper,
                          GraphToTypedInstanceMapper graphToTypedInstanceMapper) {
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
        this.typedInstanceToGraphMapper = typedInstanceToGraphMapper;
    }

    public String mapRecursive(AtlasVertex instanceVertex, boolean followReferences) throws AtlasException {
        String guid = GraphHelper.getGuid(instanceVertex);
        ITypedReferenceableInstance typedReference;
        RequestContext context = RequestContext.get();
        typedReference = context.getInstanceV1(guid);
        if (typedReference != null) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache hit: guid = {}, entityId = {}", guid, typedReference.getId()._getId());
            }
        } else {
            typedReference =
                    graphToTypedInstanceMapper.mapGraphToTypedInstance(guid, instanceVertex);
            context.cache(typedReference);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache miss: guid = {}, entityId = {}", guid, typedReference.getId().getId());
            }
        }
        String fullText = forInstance(typedReference, followReferences);
        StringBuilder fullTextBuilder =
            new StringBuilder(typedReference.getTypeName()).append(FULL_TEXT_DELIMITER).append(fullText);

        List<String> traits = typedReference.getTraits();
        for (String traitName : traits) {
            String traitText = forInstance((ITypedInstance) typedReference.getTrait(traitName), false);
            fullTextBuilder.append(FULL_TEXT_DELIMITER).append(traitName).append(FULL_TEXT_DELIMITER)
                .append(traitText);
        }
        return fullTextBuilder.toString();
    }

    private String forAttribute(IDataType type, Object value, boolean followReferences)
        throws AtlasException {
        if (value == null) {
            return null;
        }
        switch (type.getTypeCategory()) {
        case PRIMITIVE:
            return String.valueOf(value);
        case ENUM:

            return ((EnumValue) value).value;

        case ARRAY:
            StringBuilder fullText = new StringBuilder();
            IDataType elemType = ((DataTypes.ArrayType) type).getElemType();
            List list = (List) value;

            for (Object element : list) {
                String elemFullText = forAttribute(elemType, element, false);
                if (StringUtils.isNotEmpty(elemFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(elemFullText);
                }
            }
            return fullText.toString();

        case MAP:
            fullText = new StringBuilder();
            IDataType keyType = ((DataTypes.MapType) type).getKeyType();
            IDataType valueType = ((DataTypes.MapType) type).getValueType();
            Map map = (Map) value;

            for (Object entryObj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) entryObj;
                String keyFullText = forAttribute(keyType, entry.getKey(), false);
                if (StringUtils.isNotEmpty(keyFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(keyFullText);
                }
                String valueFullText = forAttribute(valueType, entry.getValue(), false);
                if (StringUtils.isNotEmpty(valueFullText)) {
                    fullText = fullText.append(FULL_TEXT_DELIMITER).append(valueFullText);
                }
            }
            return fullText.toString();

        case CLASS:
            if (followReferences) {
                Id refId = ((ITypedReferenceableInstance) value).getId();
                String refGuid = refId._getId();
                AtlasVertex refVertex = typedInstanceToGraphMapper.lookupVertex(refId);
                if(refVertex == null) {
                    refVertex = graphHelper.getVertexForGUID(refGuid);
                }
                return mapRecursive(refVertex, false);
            }
            break;

        case STRUCT:
            if (followReferences) {
                return forInstance((ITypedInstance) value, true);
            }
            break;

        default:
            throw new IllegalStateException("Unhandled type category " + type.getTypeCategory());

        }
        return null;
    }

    private String forInstance(ITypedInstance typedInstance, boolean followReferences)
        throws AtlasException {
        StringBuilder fullText = new StringBuilder();
        for (AttributeInfo attributeInfo : typedInstance.fieldMapping().fields.values()) {
            Object attrValue = typedInstance.get(attributeInfo.name);
            if (attrValue == null) {
                continue;
            }

            String attrFullText = forAttribute(attributeInfo.dataType(), attrValue, followReferences);
            if (StringUtils.isNotEmpty(attrFullText)) {
                fullText =
                    fullText.append(FULL_TEXT_DELIMITER).append(attributeInfo.name).append(FULL_TEXT_DELIMITER)
                        .append(attrFullText);
            }
        }
        return fullText.toString();
    }
}
