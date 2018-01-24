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
package org.apache.atlas.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HdfsNameServiceResolverTest {
    private HdfsNameServiceResolver hdfsNameServiceResolver = HdfsNameServiceResolver.getInstance();


    @Test
    public void testResolution() {
        assertEquals(hdfsNameServiceResolver.getNameServiceID("test"), "");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("test1"), "");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("test", 8020), "");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("test1", 8020), "");

        assertEquals(hdfsNameServiceResolver.getNameServiceID("ctr-e137-1514896590304-41888-01-000003"), "mycluster");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("ctr-e137-1514896590304-41888-01-000003", 8020), "mycluster");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("ctr-e137-1514896590304-41888-01-000004"), "mycluster");
        assertEquals(hdfsNameServiceResolver.getNameServiceID("ctr-e137-1514896590304-41888-01-000004", 8020), "mycluster");

        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://ctr-e137-1514896590304-41888-01-000004:8020/tmp/xyz"), "hdfs://mycluster/tmp/xyz");
        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://ctr-e137-1514896590304-41888-01-000004:8020/tmp/xyz/ctr-e137-1514896590304-41888-01-000004:8020"), "hdfs://mycluster/tmp/xyz/ctr-e137-1514896590304-41888-01-000004:8020");
        assertEquals(hdfsNameServiceResolver.getNameServiceIDForPath("hdfs://ctr-e137-1514896590304-41888-01-000004:8020/tmp/xyz"), "mycluster");

        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://ctr-e137-1514896590304-41888-01-000003:8020/tmp/xyz"), "hdfs://mycluster/tmp/xyz");
        assertEquals(hdfsNameServiceResolver.getNameServiceIDForPath("hdfs://ctr-e137-1514896590304-41888-01-000003:8020/tmp/xyz"), "mycluster");

        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://ctr-e137-1514896590304-41888-01-000003/tmp/xyz"), "hdfs://mycluster/tmp/xyz");
        assertEquals(hdfsNameServiceResolver.getNameServiceIDForPath("hdfs://ctr-e137-1514896590304-41888-01-000003/tmp/xyz"), "mycluster");

        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://ctr-e137-1514896590304-41888-01-000003/tmp/xyz/ctr-e137-1514896590304-41888-01-000003"), "hdfs://mycluster/tmp/xyz/ctr-e137-1514896590304-41888-01-000003");
        assertEquals(hdfsNameServiceResolver.getNameServiceIDForPath("hdfs://ctr-e137-1514896590304-41888-01-000003/tmp/xyz/ctr-e137-1514896590304-41888-01-000003"), "mycluster");

        assertEquals(hdfsNameServiceResolver.getPathWithNameServiceID("hdfs://mycluster/tmp/xyz"), "hdfs://mycluster/tmp/xyz");
        assertEquals(hdfsNameServiceResolver.getNameServiceIDForPath("hdfs://mycluster/tmp/xyz"), "mycluster");

    }
}