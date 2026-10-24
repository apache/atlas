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

import crawlercommons.domains.EffectiveTldFinder;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Rolls CrawlDb URL records up to hostname / eTLD+1. Hosts with no fetched URL are dropped.
 */
public final class HostCatalogBuilder {
    private HostCatalogBuilder() {
    }

    /**
     * Rolls CrawlDb URLs up by hostname. Drops hosts with no fetched URL.
     *
     * @param records CrawlDb records
     * @return hostname to per-crawl metrics
     */
    public static Map<String, HostMetrics> rollup(Collection<CrawlRecord> records) {
        if (records == null || records.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, HostMetrics> byHost = new LinkedHashMap<>();

        for (CrawlRecord record : records) {
            UrlParts parts = parseUrl(record.getUrl());

            if (parts == null) {
                continue;
            }

            HostMetrics metrics = byHost.get(parts.hostname);

            if (metrics == null) {
                metrics = new HostMetrics(parts.hostname, parts.protocol, parts.etldPlusOne);
                byHost.put(parts.hostname, metrics);
            }

            metrics.add(record);
        }

        Map<String, HostMetrics> fetchedOnly = new LinkedHashMap<>();

        for (Map.Entry<String, HostMetrics> entry : byHost.entrySet()) {
            if (entry.getValue().hasFetchedUrl()) {
                fetchedOnly.put(entry.getKey(), entry.getValue());
            }
        }

        return fetchedOnly;
    }

    /**
     * Builds host metrics from HostDB. Keeps hosts with {@code fetched + notModified > 0}.
     *
     * @param records HostDB rows
     * @return hostname to per-crawl metrics
     */
    public static Map<String, HostMetrics> fromHostDb(Collection<HostDbReader.HostDbRecord> records) {
        if (records == null || records.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, HostMetrics> fetchedOnly = new LinkedHashMap<>();

        for (HostDbReader.HostDbRecord record : records) {
            if (record.fetched + record.notModified <= 0) {
                continue;
            }

            UrlParts parts = parseHostname(record.hostname);

            if (parts == null) {
                continue;
            }

            HostMetrics metrics = new HostMetrics(parts.hostname, parts.protocol, parts.etldPlusOne);
            metrics.applyHostDb(record.fetched, record.notModified, record.unfetched, record.score, record.lastCheck,
                    record.dnsFailures, record.connectionFailures, record.gone, record.redirPerm, record.redirTemp,
                    record.homepageUrl);
            fetchedOnly.put(parts.hostname, metrics);
        }

        return fetchedOnly;
    }

    static UrlParts parseHostname(String hostname) {
        if (StringUtils.isBlank(hostname)) {
            return null;
        }

        String host = hostname.toLowerCase();
        if (host.charAt(host.length() - 1) == '.') {
            host = host.substring(0, host.length() - 1);
        }

        String etld = EffectiveTldFinder.getAssignedDomain(host, false, true);
        if (StringUtils.isBlank(etld)) {
            etld = host;
        }

        return new UrlParts(host, null, etld.toLowerCase());
    }

    static UrlParts parseUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return null;
        }

        try {
            URI    uri      = URI.create(url);
            String host     = uri.getHost();
            String protocol = uri.getScheme();

            if (StringUtils.isBlank(host)) {
                return null;
            }

            if (host.charAt(host.length() - 1) == '.') {
                host = host.substring(0, host.length() - 1);
            }

            host = host.toLowerCase();

            String etld = EffectiveTldFinder.getAssignedDomain(host, false, true);

            if (StringUtils.isBlank(etld)) {
                etld = host;
            }

            return new UrlParts(host, protocol, etld.toLowerCase());
        } catch (Exception e) {
            return null;
        }
    }

    static final class UrlParts {
        final String hostname;
        final String protocol;
        final String etldPlusOne;

        UrlParts(String hostname, String protocol, String etldPlusOne) {
            this.hostname    = hostname;
            this.protocol    = protocol;
            this.etldPlusOne = etldPlusOne;
        }
    }
}
