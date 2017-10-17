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
package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PredicateCondition<K, E extends TitanElement> extends Literal<E> {

    private final K key;
    private final TitanPredicate predicate;
    private final Object value;

    public PredicateCondition(K key, TitanPredicate predicate, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key instanceof String || key instanceof RelationType);
        Preconditions.checkNotNull(predicate);
        this.key = key;
        this.predicate = predicate;
        this.value = value;
    }


    private boolean satisfiesCondition(Object value) {
        return predicate.evaluate(value, this.value);
    }

    @Override
    public boolean evaluate(E element) {
        RelationType type;
        if (key instanceof String) {
            type = ((InternalElement) element).tx().getRelationType((String) key);
            if (type == null)
                return satisfiesCondition(null);
        } else {
            type = (RelationType) key;
        }

        Preconditions.checkNotNull(type);

        if (type.isPropertyKey()) {
            Iterator<Object> iter = ElementHelper.getValues(element,(PropertyKey)type).iterator();
            if (iter.hasNext()) {
                while (iter.hasNext()) {
                    if (satisfiesCondition(iter.next()))
                        return true;
                }
                return false;
            }
            return satisfiesCondition(null);
        } else {
            assert ((InternalRelationType)type).getMultiplicity().isUnique(Direction.OUT);
            return satisfiesCondition(((TitanRelation) element).getProperty((EdgeLabel) type));
        }
    }

    public K getKey() {
        return key;
    }

    public TitanPredicate getPredicate() {
        return predicate;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(key).append(predicate).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        PredicateCondition oth = (PredicateCondition) other;
        return key.equals(oth.key) && predicate.equals(oth.predicate) && compareValue(value, oth.value);
    }

    // ATLAS-2214: There's a issue when working with isNull, notNull operators
    // When string/boolean attributes use the following sequence of filtering on AtlasVertex attributes then the code
    // runs into NPE
    // 1. boolean attr "x" != false/true        | boolean attr "x" == false/true
    // 2. boolean attr notNull 'query.has("x")  | boolean attr isNull 'query.hasNot("x")'
    // whereas if the sequence is reversed then the NPE is not encountered
    // Similar behavior is exhibited for the string attributes
    // Workaround is to allow null == null value comparision
    private boolean compareValue(final Object left, final Object right) {
        return left == null ? right == null : left.equals(right);
    }

    @Override
    public String toString() {
        return key + " " + predicate + " " + String.valueOf(value);
    }

    public static <K, E extends TitanElement> PredicateCondition<K, E> of(K key, TitanPredicate titanPredicate, Object condition) {
        return new PredicateCondition<K, E>(key, titanPredicate, condition);
    }

}