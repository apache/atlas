/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.atlas.regression.util;

import org.apache.atlas.regression.request.RequestKeys;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.testng.asserts.SoftAssert;

public class TestUtils {

    public static void assertResponse(SoftAssert softAssert, int expCode, String expMsg, String
            contentType, HttpResponse response) {
        StatusLine statusLine = response.getStatusLine();
        softAssert.assertEquals(statusLine.getStatusCode(), expCode, "Status code mismatch");
        softAssert.assertEquals(statusLine.getReasonPhrase(), expMsg, "Status Message mismatch");
        softAssert.assertEquals(getContentType(response), contentType, "Content Type Mismatch");
    }

    public static String getContentType(HttpResponse response) {
        for (Header header : response.getHeaders(RequestKeys.CONTENT_TYPE_HEADER)){
            return header.getValue();
        }
        return null;
    }

    public static void assert200(SoftAssert softAssert, String contentType, HttpResponse response) {
        assertResponse(softAssert, 200, "OK", contentType, response);
    }
}
