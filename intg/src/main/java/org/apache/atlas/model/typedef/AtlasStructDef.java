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
package org.apache.atlas.model.typedef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;


/**
 * class that captures details of a struct-type.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class AtlasStructDef extends AtlasBaseTypeDef implements Serializable {
    private static final long serialVersionUID = 1L;

    // do not update this list contents directly - the list might be in the middle of iteration in another thread
    // to update list contents: 1) make a copy 2) update the copy 3) assign the copy to this member
    private List<AtlasAttributeDef> attributeDefs;

    public AtlasStructDef() {
        this(null, null, null, null);
    }

    public AtlasStructDef(String name) {
        this(name, null, null, null);
    }

    public AtlasStructDef(String name, String description) {
        this(name, description, null, null);
    }

    public AtlasStructDef(String name, String description, String typeVersion) {
        this(name, description, typeVersion, null);
    }

    public AtlasStructDef(String name, String description, String typeVersion, List<AtlasAttributeDef> attributeDefs) {
        super(name, description, typeVersion);

        setAttributeDefs(attributeDefs);
    }

    public AtlasStructDef(AtlasStructDef other) {
        super(other);

        setAttributeDefs(other != null ? other.getAttributeDefs() : null);
    }

    public List<AtlasAttributeDef> getAttributeDefs() {
        return attributeDefs;
    }

    public void setAttributeDefs(List<AtlasAttributeDef> attributeDefs) {
        if (this.attributeDefs != null && this.attributeDefs == attributeDefs) {
            return;
        }

        if (CollectionUtils.isEmpty(attributeDefs)) {
            this.attributeDefs = Collections.emptyList();
        } else {
            // if multiple attributes with same name are present, keep only the last entry
            List<AtlasAttributeDef> tmpList     = new ArrayList<AtlasAttributeDef>(attributeDefs.size());
            Set<String>             attribNames = new HashSet<String>();

            ListIterator<AtlasAttributeDef> iter = attributeDefs.listIterator(attributeDefs.size());
            while (iter.hasPrevious()) {
                AtlasAttributeDef attributeDef = iter.previous();
                String            attribName   = attributeDef != null ? attributeDef.getName() : null;

                if (attribName != null) {
                    attribName = attribName.toLowerCase();

                    if (!attribNames.contains(attribName)) {
                        tmpList.add(new AtlasAttributeDef(attributeDef));

                        attribNames.add(attribName);
                    }
                }
            }
            Collections.reverse(tmpList);

            this.attributeDefs = Collections.unmodifiableList(tmpList);
        }
    }

    public AtlasAttributeDef getAttribute(String attrName) {
        return findAttribute(this.attributeDefs, attrName);
    }

    public void addAttribute(AtlasAttributeDef attributeDef) {
        if (attributeDef == null) {
            return;
        }

        List<AtlasAttributeDef> a = this.attributeDefs;

        List<AtlasAttributeDef> tmpList = new ArrayList<AtlasAttributeDef>();
        if (CollectionUtils.isNotEmpty(a)) {
            // copy existing attributes, except ones having same name as the attribute being added
            for (AtlasAttributeDef existingAttrDef : a) {
                if (!StringUtils.equalsIgnoreCase(existingAttrDef.getName(), attributeDef.getName())) {
                    tmpList.add(existingAttrDef);
                }
            }
        }
        tmpList.add(new AtlasAttributeDef(attributeDef));

        this.attributeDefs = Collections.unmodifiableList(tmpList);
    }

    public void removeAttribute(String attrName) {
        List<AtlasAttributeDef> a = this.attributeDefs;

        if (hasAttribute(a, attrName)) {
            List<AtlasAttributeDef> tmpList = new ArrayList<AtlasAttributeDef>();

            // copy existing attributes, except ones having same name as the attribute being removed
            for (AtlasAttributeDef existingAttrDef : a) {
                if (!StringUtils.equalsIgnoreCase(existingAttrDef.getName(), attrName)) {
                    tmpList.add(existingAttrDef);
                }
            }

            this.attributeDefs = Collections.unmodifiableList(tmpList);
        }
    }

    public boolean hasAttribute(String attrName) {
        return getAttribute(attrName) != null;
    }

    private static boolean hasAttribute(List<AtlasAttributeDef> attributeDefs, String attrName) {
        return findAttribute(attributeDefs, attrName) != null;
    }

    private static AtlasAttributeDef findAttribute(List<AtlasAttributeDef> attributeDefs, String attrName) {
        AtlasAttributeDef ret = null;

        if (CollectionUtils.isNotEmpty(attributeDefs)) {
            for (AtlasAttributeDef attributeDef : attributeDefs) {
                if (StringUtils.equalsIgnoreCase(attributeDef.getName(), attrName)) {
                    ret = attributeDef;
                    break;
                }
            }
        }

        return ret;
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasStructDef{");
        super.toString(sb);
        sb.append(", attributeDefs=[");
        dumpObjects(attributeDefs, sb);
        sb.append("]");
        sb.append('}');

        return sb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }

        AtlasStructDef that = (AtlasStructDef) o;

        if (attributeDefs != null ? !attributeDefs.equals(that.attributeDefs) : that.attributeDefs != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (attributeDefs != null ? attributeDefs.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    /**
     * class that captures details of a struct-attribute.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class AtlasAttributeDef implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * single-valued attribute or multi-valued attribute.
         */
        public enum Cardinality { SINGLE, LIST, SET };

        public static final int COUNT_NOT_SET = -1;

        private String      name;
        private String      typeName;
        private boolean     isOptional;
        private Cardinality cardinality;
        private int         valuesMinCount;
        private int         valuesMaxCount;
        private boolean     isUnique;
        private boolean     isIndexable;

        public AtlasAttributeDef() { this(null, null); }

        public AtlasAttributeDef(String name, String typeName) {
            this(name, typeName, false, Cardinality.SINGLE, 1, 1, false, false);
        }

        public AtlasAttributeDef(String name, String typeName, boolean isOptional, Cardinality cardinality,
                                 int valuesMinCount, int valuesMaxCount, boolean isUnique, boolean isIndexable) {
            setName(name);
            setTypeName(typeName);
            setOptional(isOptional);
            setCardinality(cardinality);
            setValuesMinCount(valuesMinCount);
            setValuesMaxCount(valuesMaxCount);
            setUnique(isUnique);
            setIndexable(isIndexable);
        }

        public AtlasAttributeDef(AtlasAttributeDef other) {
            if (other != null) {
                setName(other.getName());
                setTypeName(other.getTypeName());
                setOptional(other.isOptional());
                setCardinality(other.getCardinality());
                setValuesMinCount(other.getValuesMinCount());
                setValuesMaxCount(other.getValuesMaxCount());
                setUnique(other.isUnique());
                setIndexable(other.isIndexable());
            }
        }


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public boolean isOptional() {
            return isOptional;
        }

        public void setOptional(boolean optional) { isOptional = optional; }

        public void setCardinality(Cardinality cardinality) {
            this.cardinality = cardinality;
        }

        public Cardinality getCardinality() {
            return cardinality;
        }

        public int getValuesMinCount() {
            return valuesMinCount;
        }

        public void setValuesMinCount(int valuesMinCount) {
            this.valuesMinCount = valuesMinCount;
        }

        public int getValuesMaxCount() {
            return valuesMaxCount;
        }

        public void setValuesMaxCount(int valuesMaxCount) {
            this.valuesMaxCount = valuesMaxCount;
        }

        public boolean isUnique() {
            return isUnique;
        }

        public void setUnique(boolean unique) {
            isUnique = unique;
        }

        public boolean isIndexable() {
            return isIndexable;
        }

        public void setIndexable(boolean idexable) {
            isIndexable = idexable;
        }


        public StringBuilder toString(StringBuilder sb) {
            if (sb == null) {
                sb = new StringBuilder();
            }

            sb.append("AtlasAttributeDef{");
            sb.append("name='").append(name).append('\'');
            sb.append(", typeName='").append(typeName).append('\'');
            sb.append(", isOptional=").append(isOptional);
            sb.append(", cardinality=").append(cardinality);
            sb.append(", valuesMinCount=").append(valuesMinCount);
            sb.append(", valuesMaxCount=").append(valuesMaxCount);
            sb.append(", isUnique=").append(isUnique);
            sb.append(", isIndexable=").append(isIndexable);
            sb.append('}');

            return sb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { return true; }
            if (o == null || getClass() != o.getClass()) { return false; }

            AtlasAttributeDef that = (AtlasAttributeDef) o;

            if (name != null ? !name.equals(that.name) : that.name != null) { return false; }
            if (typeName != null ? !typeName.equals(that.typeName) : that.typeName != null) { return false; }
            if (isOptional != that.isOptional) { return false; }
            if (cardinality != null ? !cardinality.equals(that.cardinality) : that.cardinality != null) {
                return false;
            }
            if (valuesMinCount != that.valuesMinCount) { return false; }
            if (valuesMaxCount != that.valuesMaxCount) { return false; }
            if (isUnique != that.isUnique) { return false; }
            if (isIndexable != that.isIndexable) { return false; }

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (typeName != null ? typeName.hashCode() : 0);
            result = 31 * result + (isOptional ? 1 : 0);
            result = 31 * result + (cardinality != null ? cardinality.hashCode() : 0);
            result = 31 * result + valuesMinCount;
            result = 31 * result + valuesMaxCount;
            result = 31 * result + (isUnique ? 1 : 0);
            result = 31 * result + (isIndexable ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }
    }
}
