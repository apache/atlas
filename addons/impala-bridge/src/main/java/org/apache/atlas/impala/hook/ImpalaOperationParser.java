/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.impala.hook;

import org.apache.atlas.impala.model.ImpalaOperationType;
import org.apache.commons.lang.StringUtils;

/**
 * Parse an Impala query text and output the impala operation type
 */
public class ImpalaOperationParser {

    public ImpalaOperationParser() {
    }

    public static ImpalaOperationType getImpalaOperationType(String queryText) {
        // Impala does no generate lineage record for command "LOAD DATA INPATH"
        if (StringUtils.startsWithIgnoreCase(queryText, "create view")) {
            return ImpalaOperationType.CREATEVIEW;
        } else if (StringUtils.startsWithIgnoreCase(queryText, "create table") &&
        StringUtils.containsIgnoreCase(queryText, "as select")) {
            return ImpalaOperationType.CREATETABLE_AS_SELECT;
        } else if (StringUtils.startsWithIgnoreCase(queryText, "alter view") &&
            StringUtils.containsIgnoreCase(queryText, "as select")) {
            return ImpalaOperationType.ALTERVIEW_AS;
        } else if (StringUtils.containsIgnoreCase(queryText, "insert into") &&
            StringUtils.containsIgnoreCase(queryText, "select") &&
            StringUtils.containsIgnoreCase(queryText, "from")) {
            return ImpalaOperationType.QUERY;
        } else if (StringUtils.containsIgnoreCase(queryText,"insert overwrite") &&
            StringUtils.containsIgnoreCase(queryText, "select") &&
            StringUtils.containsIgnoreCase(queryText, "from")) {
            return ImpalaOperationType.QUERY;
        }

        return ImpalaOperationType.UNKNOWN;
    }

    public static ImpalaOperationType getImpalaOperationSubType(ImpalaOperationType operationType, String queryText) {
        if (operationType == ImpalaOperationType.QUERY) {
            if (StringUtils.containsIgnoreCase(queryText, "insert into")) {
                return ImpalaOperationType.INSERT;
            } else if (StringUtils.containsIgnoreCase(queryText, "insert overwrite")) {
                return ImpalaOperationType.INSERT_OVERWRITE;
            }
        }

        return ImpalaOperationType.UNKNOWN;
    }


}