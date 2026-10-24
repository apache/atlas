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
 * See the License for the specific language governing limitations under
 * the License.
 */
package org.apache.atlas.nutch.bridge;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HostCatalogBuilderTest {
    @Test
    public void skipsHostsWithOnlyUnfetchedUrls() {
        CrawlRecord unfetched = new CrawlRecord("https://only.example.com/page", CrawlRecord.STATUS_DB_UNFETCHED, 1.0f, 1L);

        Map<String, HostMetrics> hosts = HostCatalogBuilder.rollup(Collections.singletonList(unfetched));

        assertTrue(hosts.isEmpty());
    }

    @Test
    public void importsFetchedHostsAndDerivesEtldPlusOne() {
        CrawlRecord fetched = new CrawlRecord("https://lucene.apache.org/core/", CrawlRecord.STATUS_DB_FETCHED, 2.5f, 100L);
        CrawlRecord extra   = new CrawlRecord("https://lucene.apache.org/other", CrawlRecord.STATUS_DB_UNFETCHED, 0.1f, 50L);

        Map<String, HostMetrics> hosts = HostCatalogBuilder.rollup(Arrays.asList(fetched, extra));

        assertEquals(hosts.size(), 1);
        HostMetrics metrics = hosts.get("lucene.apache.org");
        assertEquals(metrics.getEtldPlusOne(), "apache.org");
        assertEquals(metrics.getProtocol(), "https");
        assertEquals(metrics.getFetchedCount(), 1);
        assertEquals(metrics.getUnfetchedCount(), 1);
        assertEquals(metrics.getMaxScore(), 2.5f, 0.001);
        assertEquals(metrics.getLastFetchTime(), 100L);
        assertTrue(metrics.hasFetchedUrl());
    }

    @Test
    public void treatsUkPublicSuffixAsEtldPlusOne() {
        CrawlRecord fetched = new CrawlRecord("https://www.example.co.uk/a", CrawlRecord.STATUS_FETCH_SUCCESS, 1.0f, 9L);

        Map<String, HostMetrics> hosts = HostCatalogBuilder.rollup(Collections.singletonList(fetched));

        assertEquals(hosts.get("www.example.co.uk").getEtldPlusOne(), "example.co.uk");
        assertFalse("uk".equals(hosts.get("www.example.co.uk").getEtldPlusOne()));
    }

    @Test
    public void fromHostDbImportsFetchedPlusNotModifiedAndSkipsUnfetchedOnly() {
        HostDbReader.HostDbRecord fetched = new HostDbReader.HostDbRecord("lucene.apache.org", 2.5f, 100L,
                "https://lucene.apache.org/", 1L, 2L, 3L, 4L, 1L, 5L, 6L, 7L);
        HostDbReader.HostDbRecord unfetched = new HostDbReader.HostDbRecord("only.example.com", 1.0f, 1L, null, 0, 0, 9, 0, 0, 0, 0, 0);

        Map<String, HostMetrics> hosts = HostCatalogBuilder.fromHostDb(Arrays.asList(fetched, unfetched));

        assertEquals(hosts.size(), 1);
        HostMetrics metrics = hosts.get("lucene.apache.org");
        assertEquals(metrics.getEtldPlusOne(), "apache.org");
        assertEquals(metrics.getFetchedCount(), 5);
        assertEquals(metrics.getUnfetchedCount(), 3);
        assertEquals(metrics.getMaxScore(), 2.5f, 0.001);
        assertEquals(metrics.getLastFetchTime(), 100L);
        assertEquals(metrics.getDnsFailures().longValue(), 1L);
        assertEquals(metrics.getConnectionFailures().longValue(), 2L);
        assertEquals(metrics.getGoneCount().longValue(), 7L);
        assertEquals(metrics.getRedirTempCount().longValue(), 5L);
        assertEquals(metrics.getRedirPermCount().longValue(), 6L);
        assertEquals(metrics.getHomepageUrl(), "https://lucene.apache.org/");
    }
}
