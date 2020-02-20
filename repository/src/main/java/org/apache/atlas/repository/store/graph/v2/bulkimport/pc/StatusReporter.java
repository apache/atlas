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

package org.apache.atlas.repository.store.graph.v2.bulkimport.pc;

import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class StatusReporter<T, U> {
    private static final Logger LOG = LoggerFactory.getLogger(StatusReporter.class);

    private Map<T,U> producedItems = new LinkedHashMap<>();
    private Set<T> processedSet = new HashSet<>();
    private TypesUtil.Pair<T, Long> watchedItem;
    private final long timeOut;

    public StatusReporter(long timeOut) {
        this.timeOut = timeOut;
    }

    public void produced(T item, U index) {
        this.producedItems.put(item, index);
    }

    public void processed(T item) {
        this.processedSet.add(item);
    }

    public void processed(T[] index) {
        this.processedSet.addAll(Arrays.asList(index));
    }

    public U ack() {
        U ack = null;
        U ret;
        Map.Entry<T, U> firstElement;
        do {
            firstElement = getFirstElement(this.producedItems);
            ret = completionIndex(firstElement);
            if (ret != null) {
                ack = ret;
            }
        } while(ret != null);

        return addToWatchIfNeeded(ack, firstElement);
    }

    private U addToWatchIfNeeded(U ack, Map.Entry<T, U> firstElement) {
        if (ack == null && firstElement != null) {
            ack = addToWatch(firstElement.getKey());
        } else {
            resetWatchItem();
        }
        return ack;
    }

    private void resetWatchItem() {
        this.watchedItem = null;
    }

    private U addToWatch(T key) {
        createNewWatchItem(key);
        if (!hasTimedOut(this.watchedItem)) {
            return null;
        }

        T producedItemKey = this.watchedItem.left;
        resetWatchItem();
        LOG.warn("Item: {}: Was produced but not successfully processed!", producedItemKey);
        return this.producedItems.get(producedItemKey);

    }

    private void createNewWatchItem(T key) {
        if (this.watchedItem != null) {
            return;
        }

        this.watchedItem = new TypesUtil.Pair<T, Long>(key, System.currentTimeMillis());
    }

    private boolean hasTimedOut(TypesUtil.Pair<T, Long> watchedItem) {
        if (watchedItem == null) {
            return  false;
        }

        return (System.currentTimeMillis() - watchedItem.right) >= timeOut;
    }

    private Map.Entry<T, U> getFirstElement(Map<T, U> map) {
        if (map.isEmpty()) {
            return null;
        }

        return map.entrySet().iterator().next();
    }

    private U completionIndex(Map.Entry<T, U> lookFor) {
        U ack = null;
        if (lookFor == null || !processedSet.contains(lookFor.getKey())) {
            return ack;
        }

        ack = lookFor.getValue();
        producedItems.remove(lookFor.getKey());
        processedSet.remove(lookFor);
        return ack;
    }
}
