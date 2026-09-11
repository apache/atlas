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
package org.apache.atlas.server.common.filters;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.ServletRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class HeadersUtilTest {
    private Map<String, String> originalHeaders;

    @BeforeMethod
    public void setUp() {
        originalHeaders = new HashMap<>(HeadersUtil.getAllHeaders());
    }

    @AfterMethod
    public void tearDown() {
        HeadersUtil.initializeHttpResponseHeaders(convertMapToProperties(originalHeaders));
    }

    @Test
    public void testCustomCspOverrideWithNoncePlaceholder() {
        Properties props = new Properties();
        props.setProperty(HeadersUtil.CONTENT_SEC_POLICY_KEY,
                "default-src 'self'; script-src 'self' 'nonce-${nonce}'; style-src 'self' 'nonce-${nonce}';");

        HeadersUtil.initializeHttpResponseHeaders(props);
        String cspValue = HeadersUtil.getContentSecurityPolicyValue("customNonce");

        assertNotNull(cspValue);
        assertTrue(cspValue.contains("nonce-customNonce"));
        assertFalse(cspValue.contains(HeadersUtil.CONTENT_SEC_POLICY_NONCE_PLACEHOLDER));
    }

    @Test
    public void testCustomCspOverrideWithoutNoncePlaceholder() {
        String customCsp = "default-src 'self'; script-src 'self'; style-src 'self';";
        Properties props = new Properties();
        props.setProperty(HeadersUtil.CONTENT_SEC_POLICY_KEY, customCsp);

        HeadersUtil.initializeHttpResponseHeaders(props);
        String cspValue = HeadersUtil.getContentSecurityPolicyValue("ignoredNonce");

        assertEquals(cspValue, customCsp);
    }

    @Test
    public void testGenerateCspNonceProducesUniqueValues() {
        String nonceOne = HeadersUtil.generateCspNonce();
        String nonceTwo = HeadersUtil.generateCspNonce();

        assertNotNull(nonceOne);
        assertNotNull(nonceTwo);
        assertNotEquals(nonceOne, nonceTwo);
        assertTrue(nonceOne.length() >= 8);
        assertTrue(nonceTwo.length() >= 8);
    }

    @Test
    public void testDefaultTemplateReplacesScriptAndStyleNoncePlaceholders() {
        HeadersUtil.initializeHttpResponseHeaders(null);
        String cspValue = HeadersUtil.getContentSecurityPolicyValue("sharedNonce");

        assertEquals(countOccurrences(cspValue, "nonce-sharedNonce"), 2);
        assertFalse(cspValue.contains(HeadersUtil.CONTENT_SEC_POLICY_NONCE_PLACEHOLDER));
    }

    @Test
    public void testNullAndEmptyNonceFallbackBehavior() {
        HeadersUtil.initializeHttpResponseHeaders(null);

        String cspWithNullNonce = HeadersUtil.getContentSecurityPolicyValue(null);
        String cspWithEmptyNonce = HeadersUtil.getContentSecurityPolicyValue("   ");

        assertNotNull(cspWithNullNonce);
        assertNotNull(cspWithEmptyNonce);
        assertFalse(cspWithNullNonce.contains(HeadersUtil.CONTENT_SEC_POLICY_NONCE_PLACEHOLDER));
        assertFalse(cspWithEmptyNonce.contains(HeadersUtil.CONTENT_SEC_POLICY_NONCE_PLACEHOLDER));
        assertTrue(cspWithNullNonce.contains("script-src 'self' 'nonce-"));
        assertTrue(cspWithNullNonce.contains("style-src 'self' 'nonce-"));
        assertTrue(cspWithEmptyNonce.contains("script-src 'self' 'nonce-"));
        assertTrue(cspWithEmptyNonce.contains("style-src 'self' 'nonce-"));
    }

    @Test
    public void testGetOrCreateCspNonceReusesRequestScopedNonce() {
        ServletRequest request = mock(ServletRequest.class);
        Map<String, Object> attributes = new HashMap<>();

        when(request.getAttribute(anyString())).thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
        doAnswer(invocation -> {
            attributes.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(request).setAttribute(anyString(), any());

        String firstNonce = HeadersUtil.getOrCreateCspNonce(request);
        String secondNonce = HeadersUtil.getOrCreateCspNonce(request);

        assertNotNull(firstNonce);
        assertEquals(secondNonce, firstNonce);
    }

    private Properties convertMapToProperties(Map<String, String> map) {
        Properties props = new Properties();
        map.forEach(props::setProperty);
        return props;
    }

    private int countOccurrences(String value, String token) {
        int count = 0;
        int index = 0;

        while ((index = value.indexOf(token, index)) != -1) {
            count++;
            index += token.length();
        }

        return count;
    }
}
