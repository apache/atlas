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

package org.apache.hadoop.metadata.types;

public final class Multiplicity {

    public final int lower;
    public final int upper;
    public final boolean isUnique;

    public Multiplicity(int lower, int upper, boolean isUnique) {
        assert lower >= 0;
        assert upper >= 1;
        assert upper >= lower;
        this.lower = lower;
        this.upper = upper;
        this.isUnique = isUnique;
    }

    public boolean nullAllowed() {
        return lower == 0;
    }

    @Override
    public String toString() {
        return "Multiplicity{" +
                "lower=" + lower +
                ", upper=" + upper +
                ", isUnique=" + isUnique +
                '}';
    }

    public static final Multiplicity OPTIONAL = new Multiplicity(0, 1, false);
    public static final Multiplicity REQUIRED = new Multiplicity(1, 1, false);
    public static final Multiplicity COLLECTION = new Multiplicity(1, Integer.MAX_VALUE, false);
    public static final Multiplicity SET = new Multiplicity(1, Integer.MAX_VALUE, true);
}
