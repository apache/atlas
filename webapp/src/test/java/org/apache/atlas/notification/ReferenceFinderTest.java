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
package org.apache.atlas.notification;

import org.apache.atlas.notification.pc.ReferenceKeeper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ReferenceFinderTest {
    private ReferenceKeeper.ReferenceFinder referenceFinder;

    // Helpers to build currentlyProcessedEntities: qualifiedName -> {ticketKey -> 0}
    private Map<String, Map<String, Integer>> buildProcessedEntities(String qualifiedName, String... ticketKeys) {
        Map<String, Map<String, Integer>> map = new ConcurrentHashMap<>();
        addToProcessedEntities(map, qualifiedName, ticketKeys);
        return map;
    }

    private void addToProcessedEntities(Map<String, Map<String, Integer>> map, String qualifiedName, String... ticketKeys) {
        map.computeIfAbsent(qualifiedName, k -> new ConcurrentHashMap<>());
        for (String key : ticketKeys) {
            map.get(qualifiedName).put(key, 0);
        }
    }

    @BeforeMethod
    public void setup() {
        referenceFinder = new ReferenceKeeper.ReferenceFinder();
    }

    @Test
    public void testFind_withEmptyCurrentlyProcessedEntities_returnsEmpty() {
        Set<String> result = referenceFinder.find(
                Collections.emptyMap(),
                Collections.singleton("tableA@cluster"),
                Collections.singleton("dbA@cluster"));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testFind_withQualifiedNameMatch_returnsDependentTicketKey() {
        // ticket-1 is currently processing tableA@cluster
        Map<String, Map<String, Integer>> inFlight = buildProcessedEntities("tableA@cluster", "ticket-1");

        // current ticket also owns tableA@cluster
        Set<String> result = referenceFinder.find(
                inFlight,
                Collections.singleton("tableA@cluster"),
                Collections.emptySet());

        assertEquals(result.size(), 1);
        assertTrue(result.contains("ticket-1"));
    }

    @Test
    public void testFind_withReferencedSetMatch_returnsDependentTicketKey() {
        // ticket-2 is currently processing dbA@cluster
        Map<String, Map<String, Integer>> inFlight = buildProcessedEntities("dbA@cluster", "ticket-2");

        // current ticket references dbA@cluster (not as an owner, but as a reference)
        Set<String> result = referenceFinder.find(
                inFlight,
                Collections.emptySet(),
                Collections.singleton("dbA@cluster"));

        assertEquals(result.size(), 1);
        assertTrue(result.contains("ticket-2"));
    }

    @Test
    public void testFind_withNoOverlap_returnsEmpty() {
        // ticket-3 is processing an unrelated entity
        Map<String, Map<String, Integer>> inFlight = buildProcessedEntities("tableZ@cluster", "ticket-3");

        Set<String> result = referenceFinder.find(
                inFlight,
                Collections.singleton("tableA@cluster"),
                Collections.singleton("dbA@cluster"));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testFind_withMultipleTicketsOnSameEntity_returnsAllKeys() {
        // two tickets both processing tableA@cluster — current ticket must wait for both
        Map<String, Map<String, Integer>> inFlight = buildProcessedEntities("tableA@cluster", "ticket-1", "ticket-2");

        Set<String> result = referenceFinder.find(
                inFlight,
                Collections.singleton("tableA@cluster"),
                Collections.emptySet());

        assertEquals(result.size(), 2);
        assertTrue(result.contains("ticket-1"));
        assertTrue(result.contains("ticket-2"));
    }

    @Test
    public void testFind_withOverlapOnBothQNamesAndRefs_returnsUnion() {
        // ticket-1 owns tableA, ticket-2 owns dbA — current ticket overlaps with both
        Map<String, Map<String, Integer>> inFlight = new ConcurrentHashMap<>();
        addToProcessedEntities(inFlight, "tableA@cluster", "ticket-1");
        addToProcessedEntities(inFlight, "dbA@cluster",    "ticket-2");

        Set<String> qualifiedNames = Collections.singleton("tableA@cluster");
        Set<String> referencedSet  = Collections.singleton("dbA@cluster");

        Set<String> result = referenceFinder.find(inFlight, qualifiedNames, referencedSet);

        assertEquals(result.size(), 2);
        assertTrue(result.contains("ticket-1"));
        assertTrue(result.contains("ticket-2"));
    }

    @Test
    public void testFind_withPartialOverlap_returnsOnlyMatchingKeys() {
        // three entities in flight, current ticket overlaps with only one
        Map<String, Map<String, Integer>> inFlight = new ConcurrentHashMap<>();
        addToProcessedEntities(inFlight, "tableA@cluster", "ticket-1");
        addToProcessedEntities(inFlight, "tableB@cluster", "ticket-2");
        addToProcessedEntities(inFlight, "tableC@cluster", "ticket-3");

        Set<String> result = referenceFinder.find(
                inFlight,
                Collections.singleton("tableB@cluster"),
                Collections.emptySet());

        assertEquals(result.size(), 1);
        assertTrue(result.contains("ticket-2"));
    }

    @Test
    public void testFind_sameTicketKeyUnderMultipleEntities_returnedOnlyOnce() {
        // ticket-1 is processing both tableA and tableB; current ticket overlaps with both
        Map<String, Map<String, Integer>> inFlight = new ConcurrentHashMap<>();
        addToProcessedEntities(inFlight, "tableA@cluster", "ticket-1");
        addToProcessedEntities(inFlight, "tableB@cluster", "ticket-1");

        Set<String> qualifiedNames = new HashSet<>();
        qualifiedNames.add("tableA@cluster");
        qualifiedNames.add("tableB@cluster");

        Set<String> result = referenceFinder.find(inFlight, qualifiedNames, Collections.emptySet());

        // ticket-1 should appear only once even though matched via two different entities
        assertEquals(result.size(), 1);
        assertTrue(result.contains("ticket-1"));
    }
}
