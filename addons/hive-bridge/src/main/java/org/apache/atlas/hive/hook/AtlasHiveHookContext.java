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

package org.apache.atlas.hive.hook;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class AtlasHiveHookContext {
    private final HiveHook                 hook;
    private final HiveOperation            hiveOperation;
    private final HookContext              hiveContext;
    private final Hive                     hive;
    private final Map<String, AtlasEntity> qNameEntityMap = new HashMap<>();

    public AtlasHiveHookContext(HiveHook hook, HiveOperation hiveOperation, HookContext hiveContext) throws Exception {
        this.hook          = hook;
        this.hiveOperation = hiveOperation;
        this.hiveContext   = hiveContext;
        this.hive          = Hive.get(hiveContext.getConf());
    }

    public HookContext getHiveContext() {
        return hiveContext;
    }

    public Hive getHive() {
        return hive;
    }

    public HiveOperation getHiveOperation() {
        return hiveOperation;
    }

    public void putEntity(String qualifiedName, AtlasEntity entity) {
        qNameEntityMap.put(qualifiedName, entity);
    }

    public AtlasEntity getEntity(String qualifiedName) {
        return qNameEntityMap.get(qualifiedName);
    }

    public Collection<AtlasEntity> getEntities() { return qNameEntityMap.values(); }


    public String getClusterName() {
        return hook.getClusterName();
    }

    public boolean isKnownDatabase(String dbQualifiedName) {
        return hook.isKnownDatabase(dbQualifiedName);
    }

    public boolean isKnownTable(String tblQualifiedName) {
        return hook.isKnownTable(tblQualifiedName);
    }

    public void addToKnownEntities(Collection<AtlasEntity> entities) {
        hook.addToKnownEntities(entities);
    }

    public void removeFromKnownDatabase(String dbQualifiedName) {
        hook.removeFromKnownDatabase(dbQualifiedName);
    }

    public void removeFromKnownTable(String tblQualifiedName) {
        hook.removeFromKnownTable(tblQualifiedName);
    }
}
