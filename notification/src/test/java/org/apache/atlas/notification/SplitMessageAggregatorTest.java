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
package org.apache.atlas.notification;

import org.apache.atlas.model.notification.AtlasNotificationBaseMessage.CompressionKind;
import org.apache.atlas.model.notification.AtlasNotificationStringMessage;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class SplitMessageAggregatorTest {
    @Test
    public void verifyEviction() throws InterruptedException {
        Map<String, SplitMessageAggregator> map = getStringSplitMessageAggregatorMap();

        Thread.sleep(500);

        AtlasNotificationMessageDeserializer.purgeStaleMessages(map, System.currentTimeMillis(), 250);

        assertEquals(map.size(), 0);
    }

    @Test
    public void verifyEvictionDoesNotOccur() throws InterruptedException {
        Map<String, SplitMessageAggregator> map = getStringSplitMessageAggregatorMap();

        int expectedSize = map.size();

        Thread.sleep(500);

        AtlasNotificationMessageDeserializer.purgeStaleMessages(map, System.currentTimeMillis(), Long.MAX_VALUE);

        assertEquals(map.size(), expectedSize);
    }

    private Map<String, SplitMessageAggregator> getStringSplitMessageAggregatorMap() {
        Map<String, SplitMessageAggregator> map = new HashMap<>();

        map.put("1", getSplitMessageAggregator("1", 5));
        map.put("2", getSplitMessageAggregator("2", 10));

        return map;
    }

    private SplitMessageAggregator getSplitMessageAggregator(String id, int splitCount) {
        SplitMessageAggregator sma = null;

        for (int i = 0; i < splitCount; i++) {
            AtlasNotificationStringMessage sm = new AtlasNotificationStringMessage("aaaaa", id, CompressionKind.NONE, i, splitCount);

            if (sma == null) {
                sma = new SplitMessageAggregator(sm);
            } else {
                sma.add(sm);
            }
        }

        return sma;
    }
}
