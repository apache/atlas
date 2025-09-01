// Copyright 2022 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.atlas.repository.graphdb.janus.graphv3;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.janusgraph.graphdb.database.util.IndexRecordUtil.getKeyInformation;
import static org.janusgraph.graphdb.database.util.IndexRecordUtil.key2Field;

public class AtlasIndexInfoRetriever extends org.janusgraph.graphdb.database.index.IndexInfoRetriever {

    private final StandardJanusGraphTx transaction;

    public AtlasIndexInfoRetriever(StandardJanusGraphTx tx) {
        super(tx);
        Preconditions.checkNotNull(tx);
        transaction=tx;
    }

    @Override
    public KeyInformation.IndexRetriever get(final String index) {
        return new KeyInformation.IndexRetriever() {

            final Map<String,KeyInformation.StoreRetriever> indexes = new ConcurrentHashMap<>();

            @Override
            public KeyInformation get(String store, String key) {
                return get(store).get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(final String store) {
                if (indexes.get(store)==null) {
                    Preconditions.checkNotNull(transaction,"Retriever has not been initialized");
                    final MixedIndexType extIndex = IndexRecordUtil.getMixedIndex(store, transaction);
                    assert extIndex.getBackingIndexName().equals(index);
                    final ImmutableMap.Builder<String,KeyInformation> b = ImmutableMap.builder();
                    if ("edge_index".equals(store) || "vertex_index".equals(store)) {
                        Set<String> processedKeys = new HashSet<>();
                        for (final ParameterIndexField field : extIndex.getFieldKeys()) {
                            String key = key2Field(field);
                            if (!processedKeys.contains(key)) {
                                b.put(key,getKeyInformation(field));
                                processedKeys.add(key);
                            }
                        }
                    } else {
                        for (final ParameterIndexField field : extIndex.getFieldKeys()) b.put(key2Field(field),getKeyInformation(field));
                    }
                    ImmutableMap<String,KeyInformation> infoMap;
                    try {
                        infoMap = b.build();
                    } catch (IllegalArgumentException e) {
                        throw new JanusGraphException("Duplicate index field names found, likely you have multiple properties mapped to the same index field", e);
                    }
                    final KeyInformation.StoreRetriever storeRetriever = infoMap::get;
                    indexes.put(store,storeRetriever);
                }
                return indexes.get(store);
            }

            @Override
            public void invalidate(final String store) {
                indexes.remove(store);
            }
        };
    }
}