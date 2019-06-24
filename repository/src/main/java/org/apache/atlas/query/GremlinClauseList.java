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
package org.apache.atlas.query;

import org.apache.atlas.type.AtlasEntityType;

import java.util.LinkedList;
import java.util.List;

class GremlinClauseList {
    private final List<GremlinQueryComposer.GremlinClauseValue> list;

    GremlinClauseList() {
        this.list = new LinkedList<>();
    }

    public void add(GremlinQueryComposer.GremlinClauseValue g) {
        list.add(g);
    }

    public void add(int idx, GremlinQueryComposer.GremlinClauseValue g) {
        list.add(idx, g);
    }

    public void add(GremlinQueryComposer.GremlinClauseValue g, AtlasEntityType t) {
        add(g);
    }

    public void add(int idx, GremlinQueryComposer.GremlinClauseValue g, AtlasEntityType t) {
        add(idx, g);
    }

    public void add(GremlinClause clause, String... args) {
        list.add(new GremlinQueryComposer.GremlinClauseValue(clause, clause.get(args)));
    }

    public void add(int i, GremlinClause clause, String... args) {
        list.add(i, new GremlinQueryComposer.GremlinClauseValue(clause, clause.get(args)));
    }

    public GremlinQueryComposer.GremlinClauseValue getAt(int i) {
        return list.get(i);
    }

    public String getValue(int i) {
        return list.get(i).getValue();
    }

    public GremlinQueryComposer.GremlinClauseValue get(int i) {
        return list.get(i);
    }

    public int size() {
        return list.size();
    }

    public int contains(GremlinClause clause) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getClause() == clause)
                return i;
        }

        return -1;
    }

    public boolean isEmpty() {
        return list.size() == 0 || containsGVLimit();
    }

    private boolean containsGVLimit() {
        return list.size() == 3 &&
                list.get(0).getClause() == GremlinClause.G &&
                list.get(1).getClause() == GremlinClause.V &&
                list.get(2).getClause() == GremlinClause.LIMIT;
    }

    public void clear() {
        list.clear();
    }

    public GremlinQueryComposer.GremlinClauseValue remove(int index) {
        GremlinQueryComposer.GremlinClauseValue gcv = get(index);
        list.remove(index);
        return gcv;
    }
}
