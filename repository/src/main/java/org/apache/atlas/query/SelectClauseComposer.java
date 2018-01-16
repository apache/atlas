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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

class SelectClauseComposer {
    private static final String COUNT_STR = "count";
    private static final String MIN_STR = "min";
    private static final String MAX_STR = "max";
    private static final String SUM_STR = "sum";

    public boolean  isSelectNoop;

    private String[]            labels;
    private String[]            attributes; // Qualified names
    private String[]            items;
    private Map<String, String> itemAssignmentExprs;

    private int     countIdx = -1;
    private int     sumIdx   = -1;
    private int     maxIdx   = -1;
    private int     minIdx   = -1;
    private int     aggCount = 0;
    private int     introducedTypesCount = 0;
    private int     primitiveTypeCount = 0;

    public SelectClauseComposer() {}

    public static boolean isKeyword(String s) {
        return COUNT_STR.equals(s) ||
                MIN_STR.equals(s) ||
                MAX_STR.equals(s) ||
                SUM_STR.equals(s);
    }

    public String[] getItems() {
        return items;
    }

    public void setItems(final String[] items) {
        this.items = Arrays.copyOf(items, items.length);
    }

    public boolean updateAsApplicable(int currentIndex, String qualifiedName) {
        boolean ret = false;
        if (currentIndex == getCountIdx()) {
            ret = assign(currentIndex, COUNT_STR,
                    GremlinClause.INLINE_COUNT.get(), GremlinClause.INLINE_ASSIGNMENT);
        } else if (currentIndex == getMinIdx()) {
            ret = assign(currentIndex, MIN_STR, qualifiedName,
                    GremlinClause.INLINE_ASSIGNMENT, GremlinClause.INLINE_MIN);
        } else if (currentIndex == getMaxIdx()) {
            ret = assign(currentIndex, MAX_STR, qualifiedName,
                    GremlinClause.INLINE_ASSIGNMENT, GremlinClause.INLINE_MAX);
        } else if (currentIndex == getSumIdx()) {
            ret = assign(currentIndex, SUM_STR, qualifiedName,
                    GremlinClause.INLINE_ASSIGNMENT, GremlinClause.INLINE_SUM);
        } else {
            attributes[currentIndex] = qualifiedName;
        }

        return ret;
    }

    public String[] getAttributes() {
        return attributes;
    }

    public void setAttributes(final String[] attributes) {
        this.attributes = Arrays.copyOf(attributes, attributes.length);
    }

    public boolean assign(int i, String qualifiedName, GremlinClause clause) {
        items[i] = clause.get(qualifiedName);
        return true;
    }

    public String[] getLabels() {
        return labels;
    }

    public void setLabels(final String[] labels) {
        this.labels = labels;
    }

    public boolean hasAssignmentExpr() {
        return itemAssignmentExprs != null && !itemAssignmentExprs.isEmpty();
    }

    public boolean onlyAggregators() {
        return hasAggregators() && aggCount == items.length;
    }

    public boolean hasAggregators() {
        return aggCount > 0;
    }

    public String getLabelHeader() {
        return getJoinedQuotedStr(getLabels());
    }

    public String getItemsString() {
        return String.join(",", getItems());
    }

    public String getAssignmentExprString(){
        return String.join(" ", itemAssignmentExprs.values());
    }

    public String getItem(int i) {
        return items[i];
    }

    public String getAttribute(int i) {
        return attributes[i];
    }

    public String getLabel(int i) {
        return labels[i];
    }

    public int getAttrIndex(String attr) {
        int ret = -1;
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i].equals(attr)) {
                ret = i;
                break;
            }
        }
        return ret;
    }

    private boolean assign(String item, String assignExpr) {
        if (itemAssignmentExprs == null) {
            itemAssignmentExprs = new LinkedHashMap<>();
        }

        itemAssignmentExprs.put(item, assignExpr);
        return true;
    }

    private boolean assign(int i, String s, String p1, GremlinClause clause) {
        items[i] = s;
        return assign(items[i], clause.get(s, p1));

    }

    private boolean assign(int i, String s, String p1, GremlinClause inline, GremlinClause clause) {
        items[i] = s;
        return assign(items[i], inline.get(s, clause.get(p1, p1)));
    }

    public int getCountIdx() {
        return countIdx;
    }

    public void setCountIdx(final int countIdx) {
        this.countIdx = countIdx;
        aggCount++;
    }

    public int getSumIdx() {
        return sumIdx;
    }

    public void setSumIdx(final int sumIdx) {
        this.sumIdx = sumIdx;
        aggCount++;
    }

    public int getMaxIdx() {
        return maxIdx;
    }

    public void setMaxIdx(final int maxIdx) {
        this.maxIdx = maxIdx;
        aggCount++;
    }

    public int getMinIdx() {
        return minIdx;
    }

    public void setMinIdx(final int minIdx) {
        this.minIdx = minIdx;
        aggCount++;
    }

    public boolean isAggregatorIdx(int idx) {
        return getMinIdx() == idx || getMaxIdx() == idx || getCountIdx() == idx || getSumIdx() == idx;
    }

    private String getJoinedQuotedStr(String[] elements) {
        StringJoiner joiner = new StringJoiner(",");
        Arrays.stream(elements)
              .map(x -> x.contains("'") ? "\"" + x + "\"" : "'" + x + "'")
              .forEach(joiner::add);
        return joiner.toString();
    }

    public boolean isAggregatorWithArgument(int i) {
        return i == getMaxIdx() || i == getMinIdx() || i == getSumIdx();
    }

    public void incrementTypesIntroduced() {
        introducedTypesCount++;
    }

    public int getIntroducedTypesCount() {
        return introducedTypesCount;
    }

    public void incrementPrimitiveType() {
        primitiveTypeCount++;
    }

    public boolean hasMultipleReferredTypes() {
        return getIntroducedTypesCount() > 1;
    }

    public boolean hasMixedAttributes() {
        return getIntroducedTypesCount() > 0 && getPrimitiveTypeCount() > 0;
    }

    private int getPrimitiveTypeCount() {
        return primitiveTypeCount;
    }
}
