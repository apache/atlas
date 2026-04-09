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

package org.apache.atlas.v1.typesystem.types.utils;

import org.apache.atlas.v1.model.typedef.AttributeDefinition;
import org.apache.atlas.v1.model.typedef.ClassTypeDefinition;
import org.apache.atlas.v1.model.typedef.Multiplicity;
import org.apache.atlas.v1.model.typedef.TraitTypeDefinition;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TypesUtil {
    private TypesUtil() {
        // to block instantiation
    }

    public static ClassTypeDefinition createClassTypeDef(String name, String description, Set<String> superTypes, AttributeDefinition... attributes) {
        return new ClassTypeDefinition(name, description, "1.0", Arrays.asList(attributes), superTypes);
    }

    public static ClassTypeDefinition createClassTypeDef(String name, String description, String typeVersion, Set<String> superTypes, AttributeDefinition... attributes) {
        return new ClassTypeDefinition(name, description, typeVersion, Arrays.asList(attributes), superTypes);
    }

    public static TraitTypeDefinition createTraitTypeDef(String name, String description, Set<String> superTypes, AttributeDefinition... attributes) {
        return createTraitTypeDef(name, description, superTypes, Arrays.asList(attributes));
    }

    public static TraitTypeDefinition createTraitTypeDef(String name, String description, String typeVersion, Set<String> superTypes, AttributeDefinition... attributes) {
        return createTraitTypeDef(name, description, typeVersion, superTypes, Arrays.asList(attributes));
    }

    public static TraitTypeDefinition createTraitTypeDef(String name, String description, Set<String> superTypes, List<AttributeDefinition> attributes) {
        return new TraitTypeDefinition(name, description, "1.0", attributes, superTypes);
    }

    public static TraitTypeDefinition createTraitTypeDef(String name, String description, String typeVersion, Set<String> superTypes, List<AttributeDefinition> attributes) {
        return new TraitTypeDefinition(name, description, typeVersion, attributes, superTypes);
    }

    public static AttributeDefinition createUniqueRequiredAttrDef(String name, String dataTypeName) {
        return new AttributeDefinition(name, dataTypeName, Multiplicity.REQUIRED, false, true, true, null, null);
    }

    public static AttributeDefinition createRequiredAttrDef(String name, String dataTypeName) {
        return new AttributeDefinition(name, dataTypeName, Multiplicity.REQUIRED, false, false, true, null, null);
    }

    public static AttributeDefinition createOptionalAttrDef(String name, String dataTypeName) {
        return new AttributeDefinition(name, dataTypeName, Multiplicity.OPTIONAL, false, false, true, null, null);
    }

    public static class Pair<L, R> {
        public L left;
        public R right;

        public Pair(L left, R right) {
            this.left  = left;
            this.right = right;
        }

        public static <L, R> Pair<L, R> of(L left, R right) {
            return new Pair<>(left, right);
        }

        public int hashCode() {
            return Objects.hash(left, right);
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Pair<?, ?> p = (Pair<?, ?>) o;

            return Objects.equals(left, p.left) && Objects.equals(right, p.right);
        }
    }
}
