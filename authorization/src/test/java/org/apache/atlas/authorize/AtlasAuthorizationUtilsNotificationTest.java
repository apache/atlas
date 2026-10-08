/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authorize;

import org.springframework.security.core.context.SecurityContextHolder;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class AtlasAuthorizationUtilsNotificationTest {
    private static final String TOPIC_ATLAS_HOOK = "ATLAS_HOOK";

    @AfterMethod
    public void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    public void testNotificationAccessDeniedWhenTopicNull() {
        AtlasNotificationRequest request = new AtlasNotificationRequest(AtlasPrivilege.POST_NOTIFICATION, null);

        AssertJUnit.assertFalse("null topic must be denied at authorization utils layer",
                AtlasAuthorizationUtils.isAccessAllowed(request));
    }

    @Test
    public void testNotificationAccessDeniedWhenTopicBlank() {
        AtlasNotificationRequest request = new AtlasNotificationRequest(AtlasPrivilege.POST_NOTIFICATION, "  ");

        AssertJUnit.assertFalse("blank topic must be denied at authorization utils layer",
                AtlasAuthorizationUtils.isAccessAllowed(request));
    }

    @Test
    public void testNotificationAccessDeniedWhenNoAuthenticatedUser() {
        SecurityContextHolder.clearContext();

        AtlasNotificationRequest request = new AtlasNotificationRequest(AtlasPrivilege.POST_NOTIFICATION, TOPIC_ATLAS_HOOK);

        AssertJUnit.assertFalse("empty username must fail closed for notification ingress",
                AtlasAuthorizationUtils.isAccessAllowed(request));
    }
}
