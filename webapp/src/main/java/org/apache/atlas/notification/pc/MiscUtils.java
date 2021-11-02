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
package org.apache.atlas.notification.pc;

import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.Collection;
import java.util.Set;

public class MiscUtils {
    private static final String[] ENTITY_TYPE_NAME_PROCESS = new String[]{"Process", "ProcessExecution", "ddl"};

    public static void extractAllProcessTypeNames(Set<String> processTypeNames, AtlasTypeRegistry typeRegistry) {
        Collection<AtlasEntityType> typeCollection = typeRegistry.getAllEntityTypes();
        for (String processTypes : ENTITY_TYPE_NAME_PROCESS) {
            for (AtlasEntityType entityType : typeCollection) {
                if (entityType.isSubTypeOf(processTypes)) {
                    processTypeNames.add(entityType.getTypeName());
                }
            }
        }
    }
}
