/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize.simple;

import org.apache.atlas.authorize.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

import java.util.Collections;

public class AtlasSimpleAuthorizerTest {
    private static Logger LOG = LoggerFactory.getLogger(AtlasSimpleAuthorizerTest.class);

    private String          originalConf;
    private AtlasAuthorizer authorizer;

    @BeforeMethod
    public void setup1() {
        originalConf = System.getProperty("atlas.conf");

        System.setProperty("atlas.conf", "src/test/resources");

        try {
            authorizer = AtlasAuthorizerFactory.getAtlasAuthorizer();
        } catch (Exception e) {
            LOG.error("Exception in AtlasSimpleAuthorizerTest setup failed", e);
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (originalConf != null) {
            System.setProperty("atlas.conf", originalConf);
        }

        authorizer = null;
    }

    @Test(enabled = true)
    public void testAccessAllowedForUserAndGroup() {
        try {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(null, AtlasPrivilege.ENTITY_UPDATE);

            request.setUser("admin", Collections.singleton("ROLE_ADMIN"));

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);

            AssertJUnit.assertEquals(true, isAccessAllowed);
        } catch (Exception e) {
            LOG.error("Exception in AtlasSimpleAuthorizerTest", e);

            AssertJUnit.fail();
        }
    }

    @Test(enabled = true)
    public void testAccessAllowedForGroup() {
        try {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(null, AtlasPrivilege.ENTITY_UPDATE);

            request.setUser("nonmappeduser", Collections.singleton("ROLE_ADMIN"));

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);

            AssertJUnit.assertEquals(true, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            LOG.error("Exception in AtlasSimpleAuthorizerTest", e);

            AssertJUnit.fail();
        }
    }

    @Test(enabled = true)
    public void testAccessNotAllowedForUserAndGroup() {
        try {
            AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(null, AtlasPrivilege.ENTITY_UPDATE);

            request.setUser("nonmappeduser", Collections.singleton("GROUP-NOT-IN-POLICYFILE"));

            boolean isAccessAllowed = authorizer.isAccessAllowed(request);

            AssertJUnit.assertEquals(false, isAccessAllowed);
        } catch (AtlasAuthorizationException e) {
            LOG.error("Exception in AtlasSimpleAuthorizerTest", e);

            AssertJUnit.fail();
        }
    }
}
