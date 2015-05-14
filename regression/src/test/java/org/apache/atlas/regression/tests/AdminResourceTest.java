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
package org.apache.atlas.regression.tests;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.atlas.regression.request.BaseRequest;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

@Test(groups = "admin")
public class AdminResourceTest extends BaseTest {
    private static final Logger logger = Logger.getLogger(AdminResourceTest.class);
    private static String baseReqUrl = ATLAS_URL + "/api/metadata/admin";
    public SoftAssert softassert = new SoftAssert();

    @Test
    public void testVersion()
            throws Exception {
        BaseRequest req = new BaseRequest(baseReqUrl + "/version");
        HttpResponse response = req.run();
        softassert.assertEquals(200, response.getStatusLine().getStatusCode(), "Status code " +
                "mismatch");
        String json = EntityUtils.toString(response.getEntity());
        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        String version = JsonPath.read(document, "$.Version");
        String name = JsonPath.read(document, "$.Name");
        String description = JsonPath.read(document, "$.Description");
        softassert.assertTrue(null != version && !version.isEmpty(), "Version is empty");
        softassert.assertEquals(name, "metadata-governance", "Name does not match");
        softassert.assertEquals(name, "metadata-governance", "Name does not match");
        softassert.assertEquals(description,
                "Metadata Management and Data Governance Platform over " +
                        "Hadoop", "Description does not match");
        softassert.assertAll();
    }
}