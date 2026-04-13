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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification.rest;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Atlas configuration for repository project
 *
 */
public class AtlasRepositoryConfiguration {

    private static Logger LOG = LoggerFactory.getLogger(AtlasRepositoryConfiguration.class);

    public  static final String  AUDIT_EXCLUDED_OPERATIONS                              = "atlas.audit.excludes";
    public  static final String  SEPARATOR                                              = ":";


    private static List<String>  skippedOperations                  = null;


    /**
     * Get the list of operations which are configured to be skipped from auditing
     * Valid format is HttpMethod:URL eg: GET:Version
     * @return list of string
     * @throws AtlasException
     */
    public static List<String> getAuditExcludedOperations(Configuration config) throws AtlasException {
        if (skippedOperations == null) {
            if (config == null) {
                try {
                    config = ApplicationProperties.get();
                } catch (AtlasException e) {
                    LOG.error(" Error reading operations for auditing ", e);
                    throw e;
                }
            }

            skippedOperations = new ArrayList<>();

            String[] skipAuditForOperations = config.getStringArray(AUDIT_EXCLUDED_OPERATIONS);

            if (skipAuditForOperations != null && skipAuditForOperations.length > 0) {
                for (String skippedOperation : skipAuditForOperations) {
                    String[] excludedOperations = skippedOperation.trim().toLowerCase().split(SEPARATOR);
                    if (excludedOperations!= null && excludedOperations.length == 2) {
                        skippedOperations.add(skippedOperation.toLowerCase());
                    } else {
                        LOG.error("Invalid format for skipped operation {}. Valid format is HttpMethod:URL eg: GET:Version", skippedOperation);
                    }
                }
            }
        }

        return skippedOperations;
    }

    public static boolean isExcludedFromAudit(Configuration config, String httpMethod, String httpUrl) throws AtlasException {
        if (getAuditExcludedOperations(config).size() > 0) {
            return getAuditExcludedOperations(config).contains(httpMethod.toLowerCase() + SEPARATOR + httpUrl.toLowerCase());
        } else {
            return false;
        }
    }

}
