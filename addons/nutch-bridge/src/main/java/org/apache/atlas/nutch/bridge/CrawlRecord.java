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
 * One CrawlDb URL record used for host rollup. Status values match Nutch CrawlDatum.
 */
public class CrawlRecord {
    /** Page was not fetched yet. */
    public static final byte STATUS_DB_UNFETCHED = 0x01;
    /** Page was successfully fetched. */
    public static final byte STATUS_DB_FETCHED = 0x02;
    /** Page was successfully fetched and found not modified. */
    public static final byte STATUS_DB_NOTMODIFIED = 0x06;
    /** Fetching was successful. */
    public static final byte STATUS_FETCH_SUCCESS = 0x21;
    /** Fetching successful - page is not modified. */
    public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;

    private final String url;
    private final byte   status;
    private final float  score;
    private final long   fetchTime;

    /**
     * @param url       CrawlDb key (page URL)
     * @param status    Nutch {@code CrawlDatum} status byte
     * @param score     page score
     * @param fetchTime fetch time in milliseconds since epoch
     */
    public CrawlRecord(String url, byte status, float score, long fetchTime) {
        this.url       = url;
        this.status    = status;
        this.score     = score;
        this.fetchTime = fetchTime;
    }

    /** @return page URL */
    public String getUrl() {
        return url;
    }

    /** @return Nutch {@code CrawlDatum} status byte */
    public byte getStatus() {
        return status;
    }

    /** @return page score */
    public float getScore() {
        return score;
    }

    /** @return fetch time in milliseconds since epoch */
    public long getFetchTime() {
        return fetchTime;
    }

    /**
     * @return {@code true} for {@link #STATUS_DB_FETCHED}, {@link #STATUS_DB_NOTMODIFIED},
     *         {@link #STATUS_FETCH_SUCCESS}, or {@link #STATUS_FETCH_NOTMODIFIED}
     */
    public boolean isFetched() {
        return status == STATUS_DB_FETCHED
                || status == STATUS_DB_NOTMODIFIED
                || status == STATUS_FETCH_SUCCESS
                || status == STATUS_FETCH_NOTMODIFIED;
    }
}
