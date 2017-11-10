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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ImportTransformsTest {
    private final String qualifiedName  = "qualifiedName";
    private final String lowerCaseCL1   = "@cl1";
    private final String lowerCaseCL2   = "@cl2";
    private final String jsonTransforms = "{ \"hive_table\": { \"qualifiedName\":[ \"lowercase\", \"replace:@cl1:@cl2\" ] } }";

    private ImportTransforms transform;

    @BeforeTest
    public void setup() throws AtlasBaseException {
        transform = ImportTransforms.fromJson(jsonTransforms);
    }

    @Test
    public void transformEntityWith2Transforms() throws AtlasBaseException {
        AtlasEntity entity    = getHiveTableAtlasEntity();
        String      attrValue = (String) entity.getAttribute(qualifiedName);

        transform.apply(entity);

        assertEquals(entity.getAttribute(qualifiedName), applyDefaultTransform(attrValue));
    }

    @Test
    public void transformEntityWithExtInfo() throws AtlasBaseException {
        addColumnTransform(transform);

        AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntityWithExtInfo();
        AtlasEntity            entity            = entityWithExtInfo.getEntity();
        String                 attrValue         = (String) entity.getAttribute(qualifiedName);
        String[]               expectedValues    = getExtEntityExpectedValues(entityWithExtInfo);

        transform.apply(entityWithExtInfo);

        assertEquals(entityWithExtInfo.getEntity().getAttribute(qualifiedName), applyDefaultTransform(attrValue));

        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(entityWithExtInfo.getReferredEntities().get(Integer.toString(i)).getAttribute(qualifiedName), expectedValues[i]);
        }
    }

    @Test
    public void transformEntityWithExtInfoNullCheck() throws AtlasBaseException {
        addColumnTransform(transform);

        AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntityWithExtInfo();

        entityWithExtInfo.setReferredEntities(null);

        AtlasEntityWithExtInfo transformedEntityWithExtInfo = transform.apply(entityWithExtInfo);

        assertNotNull(transformedEntityWithExtInfo);
        assertEquals(entityWithExtInfo.getEntity().getGuid(), transformedEntityWithExtInfo.getEntity().getGuid());
    }

    private String[] getExtEntityExpectedValues(AtlasEntityWithExtInfo entityWithExtInfo) {
        String[] ret = new String[entityWithExtInfo.getReferredEntities().size()];

        for (int i = 0; i < ret.length; i++) {
            String attrValue = (String) entityWithExtInfo.getReferredEntities().get(Integer.toString(i)).getAttribute(qualifiedName);

            ret[i] = attrValue.replace(lowerCaseCL1, lowerCaseCL2);
        }

        return ret;
    }

    private void addColumnTransform(ImportTransforms transform) throws AtlasBaseException {
        Map<String, List<ImportTransformer>> tr     = new HashMap<>();
        List<ImportTransformer>              trList = new ArrayList<>();

        trList.add(ImportTransformer.getTransformer(String.format("replace:%s:%s", lowerCaseCL1, lowerCaseCL2)));

        tr.put(qualifiedName, trList);

        transform.getTransforms().put("hive_column", tr);
    }

    private String applyDefaultTransform(String attrValue) {
        return attrValue.toLowerCase().replace(lowerCaseCL1, lowerCaseCL2);
    }

    private AtlasEntity getHiveTableAtlasEntity() {
        AtlasEntity entity = new AtlasEntity("hive_table");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(qualifiedName, "TABLE1.default" + lowerCaseCL1);
        attributes.put("dbname", "someDB");
        attributes.put("name", "somename");

        entity.setAttributes(attributes);
        return entity;
    }

    private AtlasEntity getHiveColumnAtlasEntity(int index) {
        AtlasEntity entity = new AtlasEntity("hive_column");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(qualifiedName, String.format("col%s.TABLE1.default@cl1", index));
        attributes.put("name", "col" + index);

        entity.setAttributes(attributes);
        return entity;
    }

    private AtlasEntityWithExtInfo getAtlasEntityWithExtInfo() {
        AtlasEntityWithExtInfo ret = new AtlasEntityWithExtInfo(getHiveTableAtlasEntity());

        Map<String, AtlasEntity> referredEntities = new HashMap<>();
        referredEntities.put("0", getHiveColumnAtlasEntity(1));
        referredEntities.put("1", getHiveColumnAtlasEntity(2));
        referredEntities.put("2", getHiveColumnAtlasEntity(3));

        ret.setReferredEntities(referredEntities);

        return ret;
    }
}
