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

package org.apache.atlas.typesystem.types;

import org.apache.atlas.utils.ParamChecker;

public final class AttributeDefinition {

    public final String name;
    public final String dataTypeName;
    public final Multiplicity multiplicity;
    //A composite is the one whose lifecycle is dependent on the enclosing type and is not just a reference
    public final boolean isComposite;
    public final boolean isUnique;
    public final boolean isIndexable;

    /**
     * If this is a reference attribute, then the name of the attribute on the Class
     * that this refers to.
     */
    public final String reverseAttributeName;

    public AttributeDefinition(String name, String dataTypeName, Multiplicity multiplicity, boolean isComposite,
            String reverseAttributeName) {
        this(name, dataTypeName, multiplicity, isComposite, false, false, reverseAttributeName);

    }

    public AttributeDefinition(String name, String dataTypeName, Multiplicity multiplicity, boolean isComposite,
            boolean isUnique, boolean isIndexable, String reverseAttributeName) {
        this.name = ParamChecker.notEmpty(name, "Attribute name");
        this.dataTypeName = ParamChecker.notEmpty(dataTypeName, "Attribute type");
        this.multiplicity = multiplicity;
        this.isComposite = isComposite;
        this.isUnique = isUnique;
        this.isIndexable = isIndexable;
        this.reverseAttributeName = ParamChecker.notEmptyIfNotNull(reverseAttributeName, "Reverse attribute name");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AttributeDefinition that = (AttributeDefinition) o;

        if (isComposite != that.isComposite) {
            return false;
        }
        if (isUnique != that.isUnique) {
            return false;
        }
        if (isIndexable != that.isIndexable) {
            return false;
        }
        if (!dataTypeName.equals(that.dataTypeName)) {
            return false;
        }
        if (!multiplicity.equals(that.multiplicity)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (reverseAttributeName != null ? !reverseAttributeName.equals(that.reverseAttributeName) :
                that.reverseAttributeName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + dataTypeName.hashCode();
        result = 31 * result + multiplicity.hashCode();
        result = 31 * result + (isComposite ? 1 : 0);
        result = 31 * result + (isUnique ? 1 : 0);
        result = 31 * result + (isIndexable ? 1 : 0);
        result = 31 * result + (reverseAttributeName != null ? reverseAttributeName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return name;
    }
}
