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
package org.apache.atlas.nutch.model;

/**
 * Nutch entity type names for the <a href="https://issues.apache.org/jira/browse/ATLAS-5399">ATLAS-5399</a> model.
 */
public enum NutchDataTypes {
    /** CrawlDb crawl ({@code nutch_crawl}). */
    NUTCH_CRAWL,
    /** Seed directory for a crawl ({@code nutch_seedlist}). */
    NUTCH_SEEDLIST,
    /** Segment directory ({@code nutch_segment}). */
    NUTCH_SEGMENT,
    /** Registrable domain / eTLD+1 ({@code nutch_domain}). */
    NUTCH_DOMAIN,
    /** Hostname unique per Atlas cluster ({@code nutch_host}). */
    NUTCH_HOST,
    /** IndexingJob process created by Nutch ({@code nutch_index_process}). */
    NUTCH_INDEX_PROCESS;

    /**
     * @return Atlas type name ({@code nutch_crawl}, {@code nutch_seedlist}, ...)
     */
    public String getName() {
        return name().toLowerCase();
    }
}
