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
package org.apache.atlas.repository.store.graph.v2.glossary;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.atlas.glossary.GlossaryService.isNameInvalid;
import static org.apache.atlas.glossary.GlossaryUtils.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.glossary.Utils.*;

public class CategoryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final EntityMutations.EntityOperation operation;

    private AtlasEntityHeader anchor;
    private AtlasEntityHeader parentCategory;

    public CategoryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                EntityMutations.EntityOperation operation) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.operation = operation;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, AtlasVertex vertex) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("CategoryPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        LOG.info("CategoryPreProcessor.processAttributes: pre processing {}", AtlasType.toJson(entityStruct));

        AtlasEntity entity = (AtlasEntity) entityStruct;
        setAnchorAndParent(entity);

        switch (operation) {
            case CREATE:
                processCreateCategory(entity, vertex);
                break;
            case UPDATE:
                processUpdateCategory(entity, vertex);
                break;
        }
    }

    private void processCreateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String catName = (String) entity.getAttribute(NAME);
        String catQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(catQName));
    }

    private void processUpdateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        String catName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    String createQualifiedName(String catQualifiedName) {
        boolean parentRemoval = false; //TODO: later when parent relationship handled, review this
        String ret = "";
        String qName = "";

        if (!StringUtils.isEmpty(catQualifiedName)) {
            //extract existing nanoid for category
            String[] t1 = catQualifiedName.split("\\.");
            qName = t1[t1.length -1].split("@")[0];
        }

        qName = StringUtils.isEmpty(qName) ? getUUID() : qName;

        if (parentRemoval) {
            AtlasGlossary glossary = (AtlasGlossary) anchor.getAttribute(QUALIFIED_NAME);
            ret = qName + "@" + glossary.getQualifiedName();

        } else if (parentCategory != null) {
            String catParentName = (String) parentCategory.getAttribute(QUALIFIED_NAME);
            String[] parentCatQname = catParentName.split("@");
            ret = parentCatQname[0] + "." + qName + "@" + parentCatQname[1];

        } /*else if (cat.getParentCategory() != null) {//TODO: review later with relationship support
            AtlasGlossaryCategory parentCat = dataAccess.load(getAtlasGlossaryCategorySkeleton(cat.getParentCategory().getCategoryGuid()));
            String[] parentCatQname = parentCat.getQualifiedName().split("@");
            ret = parentCatQname[0] + "." + qName + "@" + parentCatQname[1];

        }*/ else {
            ret = qName + "@" + anchor.getAttribute(QUALIFIED_NAME);
        }

        return ret;
    }

    private boolean categoryExists(AtlasEntity category, String catQualifiedName) {
        String catName = (String) category.getAttribute(NAME);

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(Utils.ATLAS_GLOSSARY_CATEGORY_TYPENAME);
        String glossaryQName = (String) anchor.getAttribute(QUALIFIED_NAME);
        int level = getCategoryLevel(catQualifiedName);

        List<AtlasVertex> vertexList = AtlasGraphUtilsV2.glossaryFindChildByTypeAndPropertyName(entityType, catName, glossaryQName);

        //derive level, if (same level & different guid) then do not allow
        String qNameKey = entityType.getAllAttributes().get(QUALIFIED_NAME).getQualifiedName();
        for (AtlasVertex v : vertexList) {
            String vQualifiedName = v.getProperty(qNameKey, String.class);

            if (vQualifiedName.endsWith(glossaryQName)) {
                String vGuid = v.getProperty(Constants.GUID_PROPERTY_KEY, String.class);

                if (!vGuid.equals(category.getGuid())) {
                    int level2 = getCategoryLevel(vQualifiedName);
                    if (level == level2) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void setAnchorAndParent(AtlasEntity entity) throws AtlasBaseException {

        if (anchor == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(ANCHOR);

            if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                anchor = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            } else {
                anchor = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
            }
        }

        if (parentCategory == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(CATEGORY_PARENT);

            if (objectId != null) {
                if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    parentCategory = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

                } else {
                    parentCategory = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                }
            }
        }
    }

    private int getCategoryLevel(String qualifiedName){
        if (StringUtils.isEmpty(qualifiedName))
            return 1;
        return qualifiedName.split("@")[0].split("\\.").length;
    }
}
