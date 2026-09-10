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

/**
 * Per-host counters for a single crawl. Only hosts with fetchedCount &gt; 0 are imported.
 */
public class HostMetrics {
    private final String hostname;
    private final String protocol;
    private final String etldPlusOne;
    private long         fetchedCount;
    private long         unfetchedCount;
    private long         lastFetchTime;
    private float        maxScore;
    private Long         dnsFailures;
    private Long         connectionFailures;
    private Long         goneCount;
    private Long         redirPermCount;
    private Long         redirTempCount;
    private String       homepageUrl;

    /**
     * @param hostname    host name
     * @param protocol    URL scheme when known from CrawlDb; may be {@code null} for HostDB rows
     * @param etldPlusOne registrable domain / eTLD+1
     */
    public HostMetrics(String hostname, String protocol, String etldPlusOne) {
        this.hostname    = hostname;
        this.protocol    = protocol;
        this.etldPlusOne = etldPlusOne;
    }

    /**
     * Accumulates one CrawlDb URL into fetched/unfetched counts, last fetch time, and max score.
     *
     * @param record CrawlDb URL record for this host
     */
    public void add(CrawlRecord record) {
        if (record.isFetched()) {
            fetchedCount++;
            if (record.getFetchTime() > lastFetchTime) {
                lastFetchTime = record.getFetchTime();
            }
        } else {
            unfetchedCount++;
        }
        if (record.getScore() > maxScore) {
            maxScore = record.getScore();
        }
    }

    /** @return host name */
    public String getHostname() {
        return hostname;
    }

    /** @return URL scheme, or {@code null} when unknown */
    public String getProtocol() {
        return protocol;
    }

    /** @return registrable domain / eTLD+1 */
    public String getEtldPlusOne() {
        return etldPlusOne;
    }

    /** @return fetched URL count for this crawl */
    public long getFetchedCount() {
        return fetchedCount;
    }

    /** @return unfetched URL count for this crawl */
    public long getUnfetchedCount() {
        return unfetchedCount;
    }

    /** @return latest fetch time in milliseconds since epoch, or {@code 0} if unknown */
    public long getLastFetchTime() {
        return lastFetchTime;
    }

    /** @return maximum CrawlDb/HostDB score observed for this host */
    public float getMaxScore() {
        return maxScore;
    }

    /** @return {@code true} when at least one URL was fetched */
    public boolean hasFetchedUrl() {
        return fetchedCount > 0;
    }

    /**
     * Copies HostDB counters onto this host. {@code fetchedCount} is {@code fetched + notModified}.
     *
     * @param fetched            fetched URL count
     * @param notModified        not-modified URL count
     * @param unfetched          unfetched URL count
     * @param score              host score
     * @param lastCheck          last check time in milliseconds since epoch
     * @param dnsFailures        DNS failure count
     * @param connectionFailures connection failure count
     * @param gone               gone URL count
     * @param redirPerm          permanent redirect count
     * @param redirTemp          temporary redirect count
     * @param homepageUrl        optional homepage URL
     */
    public void applyHostDb(long fetched, long notModified, long unfetched, float score, long lastCheck,
            long dnsFailures, long connectionFailures, long gone, long redirPerm, long redirTemp, String homepageUrl) {
        this.fetchedCount = fetched + notModified;
        this.unfetchedCount = unfetched;
        this.maxScore = score;
        this.lastFetchTime = lastCheck;
        this.dnsFailures = dnsFailures;
        this.connectionFailures = connectionFailures;
        this.goneCount = gone;
        this.redirPermCount = redirPerm;
        this.redirTempCount = redirTemp;
        this.homepageUrl = homepageUrl;
    }

    /** @return DNS failure count from HostDB, or {@code null} when rolled up from CrawlDb */
    public Long getDnsFailures() {
        return dnsFailures;
    }

    /** @return connection failure count from HostDB, or {@code null} when rolled up from CrawlDb */
    public Long getConnectionFailures() {
        return connectionFailures;
    }

    /** @return gone URL count from HostDB, or {@code null} when rolled up from CrawlDb */
    public Long getGoneCount() {
        return goneCount;
    }

    /** @return permanent redirect count from HostDB, or {@code null} when rolled up from CrawlDb */
    public Long getRedirPermCount() {
        return redirPermCount;
    }

    /** @return temporary redirect count from HostDB, or {@code null} when rolled up from CrawlDb */
    public Long getRedirTempCount() {
        return redirTempCount;
    }

    /** @return homepage URL from HostDB, or {@code null} when unknown */
    public String getHomepageUrl() {
        return homepageUrl;
    }
}
