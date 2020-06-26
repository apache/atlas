/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.utils;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class AtlasPathExtractorUtilTest {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPathExtractorUtilTest.class);

    // Common
    private static final String METADATA_NAMESPACE          = "metaspace";
    private static final String QNAME_METADATA_NAMESPACE    = '@' + METADATA_NAMESPACE;
    private static final String SCHEME_SEPARATOR            = "://";
    private static final String ATTRIBUTE_NAME              = "name";
    private static final String ATTRIBUTE_QUALIFIED_NAME    = "qualifiedName";

    // HDFS
    private static final String HDFS_PATH_TYPE           = "hdfs_path";
    private static final String ATTRIBUTE_PATH           = "path";
    private static final String ATTRIBUTE_CLUSTER_NAME   = "clusterName";

    // Ozone
    private static final String OZONE_VOLUME      = "ozone_volume";
    private static final String OZONE_BUCKET      = "ozone_bucket";
    private static final String OZONE_KEY         = "ozone_key";
    private static final String OZONE_SCHEME      = "ofs" + SCHEME_SEPARATOR;
    private static final String OZONE_3_SCHEME    = "o3fs" + SCHEME_SEPARATOR;
    private static final String OZONE_PATH        = OZONE_SCHEME + "bucket1.volume1.ozone1/files/file.txt";
    private static final String OZONE_3_PATH      = OZONE_3_SCHEME + "bucket1.volume1.ozone1/files/file.txt";

    // HDFS
    private static final String HDFS_SCHEME    = "hdfs" + SCHEME_SEPARATOR;
    private static final String HDFS_PATH      = HDFS_SCHEME + "host_name:8020/warehouse/tablespace/external/hive/taBlE_306";

    @Test
    public void testGetPathEntityOzone3Path() {
        PathExtractorContext extractorContext = new PathExtractorContext(METADATA_NAMESPACE);

        Path path = new Path(OZONE_3_PATH);
        AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, extractorContext);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), OZONE_KEY);
        verifyOzoneKeyEntity(OZONE_3_PATH, entity);

        assertEquals(entityWithExtInfo.getReferredEntities().size(), 2);
        verifyOzoneEntities(OZONE_3_SCHEME, OZONE_3_PATH, extractorContext.getKnownEntities());

        assertEquals(extractorContext.getKnownEntities().size(), 3);
        verifyOzoneEntities(OZONE_3_SCHEME, OZONE_3_PATH, extractorContext.getKnownEntities());
    }

    @Test
    public void testGetPathEntityOzonePath() {
        PathExtractorContext extractorContext = new PathExtractorContext(METADATA_NAMESPACE);

        Path path = new Path(OZONE_PATH);
        AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, extractorContext);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), OZONE_KEY);
        verifyOzoneKeyEntity(OZONE_PATH, entity);

        assertEquals(entityWithExtInfo.getReferredEntities().size(), 2);
        verifyOzoneEntities(OZONE_SCHEME, OZONE_PATH, extractorContext.getKnownEntities());

        assertEquals(extractorContext.getKnownEntities().size(), 3);
        verifyOzoneEntities(OZONE_SCHEME, OZONE_PATH, extractorContext.getKnownEntities());
    }

    @Test
    public void testGetPathEntityHdfsPath() {
        Map<String, AtlasEntity> knownEntities = new HashMap<>();
        AtlasEntityWithExtInfo extInfo = new AtlasEntityWithExtInfo();

        PathExtractorContext extractorContext = new PathExtractorContext(METADATA_NAMESPACE);

        Path path = new Path(HDFS_PATH);
        AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, extractorContext);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), HDFS_PATH_TYPE);
        verifyHDFSEntity(entity, false);

        assertNull(extInfo.getReferredEntities());
        assertEquals(extractorContext.getKnownEntities().size(), 1);
        extractorContext.getKnownEntities().values().forEach(x -> verifyHDFSEntity(x, false));
    }

    @Test
    public void testGetPathEntityHdfsPathLowerCase() {
        PathExtractorContext extractorContext = new PathExtractorContext(METADATA_NAMESPACE, true, null);

        Path path = new Path(HDFS_PATH);
        AtlasEntityWithExtInfo entityWithExtInfo = AtlasPathExtractorUtil.getPathEntity(path, extractorContext);
        AtlasEntity entity = entityWithExtInfo.getEntity();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), HDFS_PATH_TYPE);
        verifyHDFSEntity(entity, true);

        assertNull(entityWithExtInfo.getReferredEntities());
        assertEquals(extractorContext.getKnownEntities().size(), 1);
        extractorContext.getKnownEntities().values().forEach(x -> verifyHDFSEntity(x, true));
    }

    private void verifyOzoneEntities(String scheme, String path, Map<String, AtlasEntity> knownEntities) {
        for (AtlasEntity knownEntity : knownEntities.values()) {
            switch (knownEntity.getTypeName()){
                case OZONE_KEY:
                    verifyOzoneKeyEntity(path, knownEntity);
                    break;

                case OZONE_VOLUME:
                    assertEquals(knownEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), scheme + "volume1" + QNAME_METADATA_NAMESPACE);
                    assertEquals(knownEntity.getAttribute(ATTRIBUTE_NAME), "volume1");
                    break;

                case OZONE_BUCKET:
                    assertEquals(knownEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), scheme + "volume1.bucket1" + QNAME_METADATA_NAMESPACE);
                    assertEquals(knownEntity.getAttribute(ATTRIBUTE_NAME), "bucket1");
                    break;
            }
        }
    }

    private void verifyOzoneKeyEntity(String path, AtlasEntity entity) {
        assertEquals(entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), path + QNAME_METADATA_NAMESPACE);
        assertEquals(entity.getAttribute(ATTRIBUTE_NAME), "/files/file.txt");
    }

    private void verifyHDFSEntity(AtlasEntity entity, boolean toLowerCase) {
        if (toLowerCase) {
            assertEquals(entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), HDFS_PATH.toLowerCase() + QNAME_METADATA_NAMESPACE);
            assertEquals(entity.getAttribute(ATTRIBUTE_NAME), "/warehouse/tablespace/external/hive/table_306");
            assertEquals(entity.getAttribute(ATTRIBUTE_PATH), HDFS_PATH.toLowerCase());
            assertEquals(entity.getAttribute(ATTRIBUTE_CLUSTER_NAME), METADATA_NAMESPACE);
        } else {
            assertEquals(entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), HDFS_PATH + QNAME_METADATA_NAMESPACE);
            assertEquals(entity.getAttribute(ATTRIBUTE_NAME), "/warehouse/tablespace/external/hive/taBlE_306");
            assertEquals(entity.getAttribute(ATTRIBUTE_PATH), HDFS_PATH);
            assertEquals(entity.getAttribute(ATTRIBUTE_CLUSTER_NAME), METADATA_NAMESPACE);
        }
    }
}
