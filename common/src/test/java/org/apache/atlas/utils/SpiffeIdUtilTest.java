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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SpiffeIdUtilTest {
    private static final String VALID = "spiffe://my-cluster/ns/service-namespace/sa/service-sa";

    @Test
    public void isValidSpiffeId_acceptsExactFormat() {
        assertTrue(SpiffeIdUtil.isValidSpiffeId(VALID));
        assertTrue(SpiffeIdUtil.isValidSpiffeId("  " + VALID + "  "));
    }

    @Test
    public void isValidSpiffeId_rejectsMalformed() {
        assertFalse(SpiffeIdUtil.isValidSpiffeId(null));
        assertFalse(SpiffeIdUtil.isValidSpiffeId(""));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("   "));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("not-a-spiffe-id"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("https://my-cluster/ns/n/sa/s"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/sa/service-sa"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/n"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns//sa/s"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe:///ns/n/sa/s"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/ns/n/sa/"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId(VALID + "/"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId(VALID + "/extra"));
        assertFalse(SpiffeIdUtil.isValidSpiffeId("spiffe://my-cluster/sa/s/ns/n"));
    }

    @Test
    public void extractServiceAccount_returnsSaOnlyForValidId() {
        assertEquals(SpiffeIdUtil.extractServiceAccount(VALID), "service-sa");
        assertEquals(SpiffeIdUtil.extractServiceAccount("  " + VALID + "  "), "service-sa");
    }

    @Test
    public void extractServiceAccount_returnsNullForMalformedId() {
        assertNull(SpiffeIdUtil.extractServiceAccount(null));
        assertNull(SpiffeIdUtil.extractServiceAccount(""));
        assertNull(SpiffeIdUtil.extractServiceAccount("not-a-spiffe-id"));
        assertNull(SpiffeIdUtil.extractServiceAccount("spiffe://my-cluster/sa/service-sa"));
        assertNull(SpiffeIdUtil.extractServiceAccount("spiffe://my-cluster/ns/n/sa/"));
        assertNull(SpiffeIdUtil.extractServiceAccount(VALID + "/extra"));
    }

    @Test
    public void rejectsSpecValidButNonConformingSpiffeId() {
        String workloadPath = "spiffe://example.org/workload/frontend";
        String nsOnly       = "spiffe://example.org/ns/prod";

        assertFalse(SpiffeIdUtil.isValidSpiffeId(workloadPath));
        assertFalse(SpiffeIdUtil.isValidSpiffeId(nsOnly));
        assertNull(SpiffeIdUtil.extractServiceAccount(workloadPath));
        assertNull(SpiffeIdUtil.extractServiceAccount(nsOnly));
    }

    @Test
    public void parseHeaderNames_handlesSingleAndMultiple() {
        assertTrue(SpiffeIdUtil.parseHeaderNames((String) null).isEmpty());
        assertTrue(SpiffeIdUtil.parseHeaderNames("   ").isEmpty());
        assertEquals(SpiffeIdUtil.parseHeaderNames("x-awc-source-workload-id"),
                Collections.singletonList("x-awc-source-workload-id"));

        List<String> names = SpiffeIdUtil.parseHeaderNames(" x-source , x-upstream ,");

        assertEquals(names, Arrays.asList("x-source", "x-upstream"));
    }

    @Test
    public void parseHeaderNames_fromStringArray_matchesGetStringArraySemantics() {
        assertTrue(SpiffeIdUtil.parseHeaderNames((String[]) null).isEmpty());
        assertTrue(SpiffeIdUtil.parseHeaderNames(new String[0]).isEmpty());

        List<String> names = SpiffeIdUtil.parseHeaderNames(new String[] {"x-source", "x-upstream"});

        assertEquals(names, Arrays.asList("x-source", "x-upstream"));
    }
}
