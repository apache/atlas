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
 * Hive-style qualifiedName helpers for Nutch types.
 */
public final class NutchQualifiedNames {
    private NutchQualifiedNames() {
    }

    /**
     * @param crawlId     Nutch crawl id
     * @param clusterName Atlas cluster name
     * @return {@code {crawlId}@{clusterName}}
     */
    public static String crawl(String crawlId, String clusterName) {
        return crawlId + "@" + clusterName;
    }

    /**
     * @param crawlId     Nutch crawl id
     * @param clusterName Atlas cluster name
     * @return {@code {crawlId}.seeds@{clusterName}}
     */
    public static String seedlist(String crawlId, String clusterName) {
        return crawlId + ".seeds@" + clusterName;
    }

    /**
     * @param crawlId     Nutch crawl id
     * @param segmentName Nutch segment directory name
     * @param clusterName Atlas cluster name
     * @return {@code {crawlId}.{segmentName}@{clusterName}}
     */
    public static String segment(String crawlId, String segmentName, String clusterName) {
        return crawlId + "." + segmentName + "@" + clusterName;
    }

    /**
     * @param etldPlusOne registrable domain / eTLD+1
     * @param clusterName Atlas cluster name
     * @return {@code {etldPlusOne}@{clusterName}}
     */
    public static String domain(String etldPlusOne, String clusterName) {
        return etldPlusOne + "@" + clusterName;
    }

    /**
     * @param hostname    host name
     * @param clusterName Atlas cluster name
     * @return {@code {hostname}@{clusterName}}
     */
    public static String host(String hostname, String clusterName) {
        return hostname + "@" + clusterName;
    }
}
