/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.query;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
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

    public static Info create(GremlinQueryComposer.Context context,
                              org.apache.atlas.query.Lookup lookup,
                              String identifier) {
        Info idInfo = new Info(identifier);
        idInfo.update(lookup, context);
        return idInfo;
    }

    private static String extract(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : s;
    }

    public static String getQualifiedName(org.apache.atlas.query.Lookup lookup,
                                          GremlinQueryComposer.Context context,
                                          String name) {
        try {
            return lookup.getQualifiedName(context, name);
        } catch (AtlasBaseException e) {
            context.error(e, AtlasErrorCode.INVALID_DSL_QUALIFIED_NAME, context.getActiveTypeName(), name);
        }

        return "";
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

    public static String getFixedRegEx(String s) {
        return s.replace("*", ".*").replace('?', '.');
    }


    public static class Info {
        private String   raw;
        private String   actual;
        private String[] parts;
        private String   typeName;
        private String   attributeName;
        private boolean  isPrimitive;
        private String   edgeLabel;
        private boolean  introduceType;
        private boolean  hasSubtypes;
        private String   subTypes;
        private boolean  isTrait;
        private boolean  newContext;
        private boolean  isAttribute;
        private String   qualifiedName;
        private boolean  isDate;
        private boolean  isNumeric;

        public Info(String s) {
            this.raw = removeQuotes(s);
            this.actual = IdentifierHelper.get(raw);
        }

        private void update(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            try {
                newContext = context.isEmpty();
                if (!newContext) {
                    if (context.hasAlias(this.raw)) {
                        raw = context.getTypeNameFromAlias(this.raw);
                    }

                    updateParts();
                    updateTypeInfo(lookup, context);
                    setIsTrait(context, lookup, attributeName);
                    updateEdgeInfo(lookup, context);
                    introduceType = !isPrimitive() && !context.hasAlias(parts[0]);
                    updateSubTypes(lookup, context);
                }
            } catch (NullPointerException ex) {
                context.getErrorList().add(ex.getMessage());
            }
        }

        private void setIsTrait(GremlinQueryComposer.Context ctx, Lookup lookup, String s) {
            isTrait = lookup.isTraitType(s);
        }

        private void updateSubTypes(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if (isTrait) {
                return;
            }

            hasSubtypes = lookup.doesTypeHaveSubTypes(context);
            if (hasSubtypes) {
                subTypes = lookup.getTypeAndSubTypes(context);
            }
        }

        private void updateEdgeInfo(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if (!isPrimitive && !isTrait && typeName != attributeName) {
                edgeLabel = lookup.getRelationshipEdgeLabel(context, attributeName);
                typeName = lookup.getTypeFromEdge(context, attributeName);
            }
        }

        private void updateTypeInfo(org.apache.atlas.query.Lookup lookup, GremlinQueryComposer.Context context) {
            if (parts.length == 1) {
                typeName = context.hasAlias(parts[0]) ?
                                   context.getTypeNameFromAlias(parts[0]) :
                                   context.getActiveTypeName();
                qualifiedName = getDefaultQualifiedNameForSinglePartName(context, parts[0]);
                attributeName = parts[0];
            }

            if (parts.length == 2) {
                boolean isAttrOfActiveType = lookup.hasAttribute(context, parts[0]);
                if (isAttrOfActiveType) {
                    attributeName = parts[0];
                } else {
                    typeName = context.hasAlias(parts[0]) ?
                                       context.getTypeNameFromAlias(parts[0]) :
                                       parts[0];

                    attributeName = parts[1];
                }
            }

            isAttribute = lookup.hasAttribute(context, attributeName);
            isPrimitive = lookup.isPrimitive(context, attributeName);
            setQualifiedName(lookup, context, isAttribute, attributeName);
            setIsDate(lookup, context, isPrimitive, attributeName);
            setIsNumeric(lookup, context, isPrimitive, attributeName);
        }

        private String getDefaultQualifiedNameForSinglePartName(GremlinQueryComposer.Context context, String s) {
            String qn = context.getTypeNameFromAlias(s);
            if (StringUtils.isEmpty(qn) && SelectClauseComposer.isKeyword(s)) {
                return s;
            }

            return qn;
        }

        private void setQualifiedName(Lookup lookup, GremlinQueryComposer.Context context, boolean isAttribute, String attrName) {
            if (isAttribute) {
                qualifiedName = getQualifiedName(lookup, context, attrName);
            }
        }

        private String getQualifiedName(Lookup lookup, GremlinQueryComposer.Context context, String name) {
            return IdentifierHelper.getQualifiedName(lookup, context, name);
        }

        private void setIsDate(Lookup lookup, GremlinQueryComposer.Context context, boolean isPrimitive, String attrName) {
            if (isPrimitive) {
                isDate = lookup.isDate(context, attrName);
            }
        }

        private void setIsNumeric(Lookup lookup, GremlinQueryComposer.Context context, boolean isPrimitive, String attrName) {
            if (isPrimitive) {
                isNumeric = lookup.isNumeric(context, attrName);
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

        public boolean isReferredType() {
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

        public String getRaw() {
            return raw;
        }

        public boolean isNumeric() {
            return isNumeric;
        }
    }
}
