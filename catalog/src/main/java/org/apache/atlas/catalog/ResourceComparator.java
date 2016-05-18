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

package org.apache.atlas.catalog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Provides key ordering for resource property maps.
 * Ordering can be defined explicitly for specific properties,
 * otherwise natural ordering is used.
 */
public class ResourceComparator implements Comparator<String> {
    private static List<String> ordering = new ArrayList<>();

    @Override
    public int compare(String s1, String s2) {
       if (s1.equals(s2)) {
            return 0;
        }

        int s1Order = ordering.indexOf(s1);
        int s2Order = ordering.indexOf(s2);

        if (s1Order == -1 && s2Order == -1) {
            return s1.compareTo(s2);
        }

        if (s1Order != -1 && s2Order != -1) {
            return s1Order - s2Order;
        }

        return s1Order == -1 ? 1 : -1;

    }

    //todo: each resource definition can provide its own ordering list
    static {
        ordering.add("href");
        ordering.add("name");
        ordering.add("id");
        ordering.add("description");
        ordering.add("type");
    }
}
