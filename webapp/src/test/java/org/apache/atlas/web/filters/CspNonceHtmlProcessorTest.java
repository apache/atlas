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
package org.apache.atlas.web.filters;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class CspNonceHtmlProcessorTest {
    private final CspNonceHtmlProcessor processor = new CspNonceHtmlProcessor();

    @Test
    public void testInjectNonceUpdatesMetaAndAddsScriptStyleNonceAttributes() {
        String html = "<html><head><meta name=\"csp-nonce\" content=\"\" /></head><body>"
                + "<script>console.log('a');</script>"
                + "<style>.x{color:red;}</style>"
                + "</body></html>";

        String output = processor.injectNonce(html, "nonce123");

        assertTrue(output.contains("<meta name=\"csp-nonce\" content=\"nonce123\" />"));
        assertTrue(output.contains("<script nonce=\"nonce123\">"));
        assertTrue(output.contains("<style nonce=\"nonce123\">"));
    }

    @Test
    public void testInjectNonceAddsMetaWhenMissing() {
        String html = "<html><head><title>Atlas</title></head><body><script src=\"app.js\"></script></body></html>";

        String output = processor.injectNonce(html, "nonce456");

        assertTrue(output.contains("<meta name=\"csp-nonce\" content=\"nonce456\" />"));
        assertTrue(output.contains("<script src=\"app.js\" nonce=\"nonce456\"></script>"));
    }

    @Test
    public void testInjectNonceDoesNotOverrideExistingScriptNonce() {
        String html = "<html><head></head><body><script nonce=\"existing\">x()</script></body></html>";

        String output = processor.injectNonce(html, "nonce789");

        assertTrue(output.contains("<script nonce=\"existing\">x()</script>"));
        assertFalse(output.contains("nonce=\"nonce789\""));
    }

    @Test
    public void testInjectNonceReturnsInputWhenNonceBlank() {
        String html = "<html><head><title>Atlas</title></head><body></body></html>";

        assertEquals(processor.injectNonce(html, "   "), html);
        assertEquals(processor.injectNonce(html, null), html);
    }
}
