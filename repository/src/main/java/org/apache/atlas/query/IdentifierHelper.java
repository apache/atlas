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

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdentifierHelper {

    private static final Pattern SINGLE_QUOTED_IDENTIFIER   = Pattern.compile("'(\\w[\\w\\d\\.\\s]*)'");
    private static final Pattern DOUBLE_QUOTED_IDENTIFIER   = Pattern.compile("\"(\\w[\\w\\d\\.\\s]*)\"");
    private static final Pattern BACKTICK_QUOTED_IDENTIFIER = Pattern.compile("`(\\w[\\w\\d\\.\\s]*)`");

    public static String get(String quotedIdentifier) {
        String ret;

        if (quotedIdentifier.charAt(0) == '`') {
            ret = extract(BACKTICK_QUOTED_IDENTIFIER, quotedIdentifier);
        } else if (quotedIdentifier.charAt(0) == '\'') {
            ret = extract(SINGLE_QUOTED_IDENTIFIER, quotedIdentifier);
        } else if (quotedIdentifier.charAt(0) == '"') {
            ret = extract(DOUBLE_QUOTED_IDENTIFIER, quotedIdentifier);
        } else {
            ret = quotedIdentifier;
        }

        return ret;
    }

    public static Advice create(GremlinQueryComposer.Context context,
                                org.apache.atlas.query.Lookup lookup,
                                String identifier) {
        Advice ia = new Advice(identifier);
        ia.update(lookup, context);
        return ia;
    }

    private static String extract(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : s;
    }

    public static String getQualifiedName(org.apache.atlas.query.Lookup lookup,
                                          GremlinQueryComposer.Context context,
                                          String name) {
        return lookup.getQualifiedName(context, name);
    }

    public static boolean isQuoted(String val) {
        boolean ret = false;

        if (val != null && val.length() > 1) {
            char first = val.charAt(0);
            char last  = val.charAt(val.length() - 1);

            if (first == last && (first == '\'' || first == '"' || first == '`')) {
                ret = true;
            }
        }

        return ret;
    }

    public static String removeQuotes(String rhs) {
        return rhs.replace("\"", "")
                  .replace("'", "")
                  .replace("`", "");
    }

    public static String getQuoted(String s) {
        return String.format("'%s'", s);
    }

    public static boolean isTrueOrFalse(String rhs) {
        return rhs.equalsIgnoreCase("true") || rhs.equalsIgnoreCase("false");
    }

    public static class Advice {
        private String raw;
        private String actual;
        private String[] parts;
        private String typeName;
        private String attributeName;
        private boolean isPrimitive;
        private String edgeLabel;
        private String edgeDirection;
        private boolean introduceType;
        private boolean hasSubtypes;
        private String subTypes;
        private boolean isTrait;
        private boolean newContext;
        private boolean isAttribute;
        private String qualifiedName;
        private boolean isDate;

        public Advice(String s) {
            this.raw = removeQuotes(s);
            this.actual = IdentifierHelper.get(raw);
        }

        private void update(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            newContext = context.isEmpty();
            if(!newContext) {
                if(context.aliasMap.containsKey(this.raw)) {
                    raw = context.aliasMap.get(this.raw);
                }

                updateParts();
                updateTypeInfo(lookup, context);
                isTrait = lookup.isTraitType(context);
                updateEdgeInfo(lookup, context);
                introduceType = !isPrimitive() && !context.hasAlias(parts[0]);
                updateSubTypes(lookup, context);
            }
        }

        private void updateSubTypes(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if(isTrait) {
                return;
            }

            hasSubtypes = lookup.doesTypeHaveSubTypes(context);
            if(hasSubtypes) {
                subTypes = lookup.getTypeAndSubTypes(context);
            }
        }

        private void updateEdgeInfo(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if(isPrimitive == false && isTrait == false) {
                edgeLabel = lookup.getRelationshipEdgeLabel(context, attributeName);
                edgeDirection = "OUT";
                typeName = lookup.getTypeFromEdge(context, attributeName);
            }
        }

        private void updateTypeInfo(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if(parts.length == 1) {
                typeName = context.getActiveTypeName();
                attributeName = parts[0];
                isAttribute = lookup.hasAttribute(context, typeName);
                qualifiedName = lookup.getQualifiedName(context, attributeName);
                isPrimitive = lookup.isPrimitive(context, attributeName);

                setIsDate(lookup, context);
            }

            if(parts.length == 2) {
                if(context.hasAlias(parts[0])) {
                    typeName = context.getTypeNameFromAlias(parts[0]);
                    attributeName = parts[1];
                    isPrimitive = lookup.isPrimitive(context, attributeName);
                    setIsDate(lookup, context);
                }
                else {
                    isAttribute = lookup.hasAttribute(context, parts[0]);
                    if(isAttribute) {
                        attributeName = parts[0];
                        isPrimitive = lookup.isPrimitive(context, attributeName);
                        setIsDate(lookup, context);
                    } else {
                        typeName = parts[0];
                        attributeName = parts[1];
                        isPrimitive = lookup.isPrimitive(context, attributeName);
                        setIsDate(lookup, context);
                    }
                }

                qualifiedName = lookup.getQualifiedName(context, attributeName);
            }
        }

        private void setIsDate(Lookup lookup, GremlinQueryComposer.Context context) {
            if(isPrimitive) {
                isDate = lookup.isDate(context, attributeName);
            }
        }

        private void updateParts() {
            parts = StringUtils.split(raw, ".");
        }

        public String getQualifiedName() {
            return qualifiedName;
        }

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public boolean isAttribute() {
            return isAttribute;
        }

        public String getAttributeName() {
            return attributeName;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getTypeName() {
            return typeName;
        }

        public boolean getIntroduceType() {
            return introduceType;
        }

        public boolean isTrait() {
            return isTrait;
        }

        public boolean hasSubtypes() {
            return hasSubtypes;
        }

        public String getSubTypes() {
            return subTypes;
        }

        public String get() {
            return actual;
        }

        public boolean isDate() {
            return isDate;
        }

        public boolean hasParts() {
            return parts.length > 1;
        }
     }
}
