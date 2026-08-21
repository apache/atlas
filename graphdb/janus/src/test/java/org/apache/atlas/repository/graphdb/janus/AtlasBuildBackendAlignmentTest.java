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
package org.apache.atlas.repository.graphdb.janus;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AtlasBuildBackendAlignmentTest {
    @Test
    public void testStorageBackendCompatibility() {
        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("hbase", "hbase2"));
        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("hbase", "hbase"));
        assertFalse(AtlasJanusGraphDatabase.isStorageBackendCompatible("hbase", "rdbms"));

        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("cassandra", "cql"));
        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("cassandra", "embeddedcassandra"));
        assertFalse(AtlasJanusGraphDatabase.isStorageBackendCompatible("cassandra", "hbase2"));

        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("rdbms", "rdbms"));
        assertFalse(AtlasJanusGraphDatabase.isStorageBackendCompatible("rdbms", "cql"));

        assertTrue(AtlasJanusGraphDatabase.isStorageBackendCompatible("berkeleyje", "berkeleyje"));
    }

    @Test
    public void testIndexBackendCompatibility() {
        assertTrue(AtlasJanusGraphDatabase.isIndexBackendCompatible("solr", "solr"));
        assertFalse(AtlasJanusGraphDatabase.isIndexBackendCompatible("solr", "elasticsearch"));
        assertTrue(AtlasJanusGraphDatabase.isIndexBackendCompatible("elasticsearch", "elasticsearch"));
    }
}
