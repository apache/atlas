/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.graphdb.types.ParameterType;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Unit tests for the unique persisted {@code MAPPED_NAME} lifecycle (Approach A). A stateful fake of the Janus mixed
 * index records every {@code addIndexKey} so that delete/recreate, restart and datatype scenarios exercise the real
 * collision-resolution code in {@link AtlasJanusGraphManagement}.
 */
public class AtlasJanusGraphManagementMappedNameTest {
    private static final String INDEX_NAME = "vertex_index";

    /** Stateful stand-in for a Janus mixed index: key -> registered parameters. */
    private static final class FakeMixedIndex {
        final List<PropertyKey>   keys   = new ArrayList<>();
        final List<Parameter<?>[]> params = new ArrayList<>();

        final JanusGraphIndex index = mock(JanusGraphIndex.class);

        FakeMixedIndex() {
            when(index.getFieldKeys()).thenAnswer(inv -> keys.toArray(new PropertyKey[0]));
            when(index.getParametersFor(any(PropertyKey.class))).thenAnswer(inv -> params.get(keys.indexOf(inv.<PropertyKey>getArgument(0))));
        }

        void register(PropertyKey key, String mappedName) {
            keys.add(key);
            params.add(new Parameter<?>[] {ParameterType.MAPPED_NAME.getParameter(mappedName)});
        }
    }

    /** One Atlas-side management session over the fake index; {@code defaultNames} drives the Janus default field name. */
    private static final class Session {
        final JanusGraphManagement janusMgmt;
        // The default Janus field name (index.search.map-name=true) is the key name, for any number of varargs parameters.
        final AtlasJanusGraph      graph     = mock(AtlasJanusGraph.class, Mockito.withSettings().defaultAnswer(inv ->
                "getIndexFieldName".equals(inv.getMethod().getName()) ? ((AtlasJanusPropertyKey) inv.getArgument(0)).getName() : null));
        final AtlasJanusGraphManagement mgmt;

        Session(FakeMixedIndex fake) {
            // A default answer (rather than an argument-matcher stub) records addIndexKey for any number of varargs
            // parameters, including none.
            janusMgmt = mock(JanusGraphManagement.class, Mockito.withSettings().defaultAnswer(inv -> {
                if (!"addIndexKey".equals(inv.getMethod().getName())) {
                    return null;
                }

                PropertyKey key = inv.getArgument(1);
                List<Parameter<?>> given = new ArrayList<>();

                for (int i = 2; i < inv.getArguments().length; i++) {
                    Object a = inv.getArguments()[i];

                    if (a instanceof Parameter[]) {
                        for (Parameter<?> p : (Parameter<?>[]) a) {
                            given.add(p);
                        }
                    } else if (a instanceof Parameter) {
                        given.add((Parameter<?>) a);
                    }
                }

                boolean hasMapped = false;

                for (Parameter<?> p : given) {
                    hasMapped |= ParameterType.MAPPED_NAME.getName().equals(p.key());
                }

                if (!hasMapped) {
                    given.add(ParameterType.MAPPED_NAME.getParameter(key.name()));
                }

                fake.keys.add(key);
                fake.params.add(given.toArray(new Parameter<?>[0]));

                return null;
            }));

            when(janusMgmt.getGraphIndex(INDEX_NAME)).thenReturn(fake.index);

            mgmt = new AtlasJanusGraphManagement(graph, janusMgmt);
        }

        AtlasJanusPropertyKey key(String name) {
            PropertyKey janusKey = mock(PropertyKey.class);

            when(janusKey.name()).thenReturn(name);

            AtlasJanusPropertyKey atlasKey = mock(AtlasJanusPropertyKey.class);

            when(atlasKey.getWrappedPropertyKey()).thenReturn(janusKey);
            when(atlasKey.getName()).thenReturn(name);

            return atlasKey;
        }
    }

    private static <T> T withBackend(String backend, java.util.concurrent.Callable<T> body) throws Exception {
        Configuration config = mock(Configuration.class);

        when(config.getString(ApplicationProperties.INDEX_BACKEND_CONF)).thenReturn(backend);

        try (MockedStatic<ApplicationProperties> props = Mockito.mockStatic(ApplicationProperties.class)) {
            props.when(ApplicationProperties::get).thenReturn(config);
            return body.call();
        }
    }

    private static String add(Session s, AtlasJanusPropertyKey key) {
        return s.mgmt.addMixedIndex(INDEX_NAME, key, false, false);
    }

    @Test
    public void firstRegistrationKeepsExistingName() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo");
    }

    @Test
    public void deleteAndRecreateGetsNextFreeSuffix() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        // retired key (renamed to foo_deleted_0 in Janus) still owns mapped name "foo"
        PropertyKey retired = mock(PropertyKey.class);
        fake.register(retired, "foo");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__1");
    }

    @Test
    public void repeatedRecreationKeepsIncrementing() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__1");
        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__2");
        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__3");
    }

    @Test
    public void occupiedSuffixesAreSkipped() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");
        fake.register(mock(PropertyKey.class), "foo__1");
        fake.register(mock(PropertyKey.class), "foo__2");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__3");
    }

    @Test
    public void gapInSuffixesPicksLowestFree() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");
        fake.register(mock(PropertyKey.class), "foo__2");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo"))), "foo__1");
    }

    @Test
    public void persistedMappedNameIsAuthoritativeForRegisteredKey() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        AtlasJanusPropertyKey recreated = s.key("foo");

        assertEquals(withBackend("opensearch", () -> add(s, recreated)), "foo__1");

        // getIndexFieldName() must return the persisted name, not the name-derived default "foo"
        assertEquals(withBackend("opensearch", () -> s.mgmt.getIndexFieldName(INDEX_NAME, recreated, false, false)), "foo__1");
    }

    @Test
    public void restartReopenPreservesNames() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        first = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        AtlasJanusPropertyKey recreated = first.key("foo");

        assertEquals(withBackend("opensearch", () -> add(first, recreated)), "foo__1");

        // "restart": brand-new management/graph objects over the same persisted index metadata
        Session reopened = new Session(fake);

        assertEquals(withBackend("opensearch", () -> reopened.mgmt.getIndexFieldName(INDEX_NAME, recreated, false, false)), "foo__1");
    }

    @Test
    public void differentPropertyNamesDoNotCollide() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("bar"))), "bar");
        assertEquals(withBackend("opensearch", () -> add(s, s.key("foo_other"))), "foo_other");
    }

    @Test
    public void stringToLongAndLongToStringRecreationGetDistinctFields() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        // original String attribute foo, then recreated as Long (foo__1), then back to String (foo__2)
        fake.register(mock(PropertyKey.class), "foo");

        String asLong   = withBackend("opensearch", () -> add(s, s.key("foo")));
        String asString = withBackend("opensearch", () -> s.mgmt.addMixedIndex(INDEX_NAME, s.key("foo"), true, false));

        assertEquals(asLong, "foo__1");
        assertEquals(asString, "foo__2");
    }

    @Test
    public void collisionIsDetectedOnThePhysicalFieldNotTheRawMappedName() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        // OpenSearchIndex#mapKey2Field turns every '.' and ' ' into the bullet, so these two share one physical field
        fake.register(mock(PropertyKey.class), "a\u2022b");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("a.b"))), "a.b__1");

        fake.register(mock(PropertyKey.class), "c d");

        assertEquals(withBackend("opensearch", () -> add(s, s.key("c.d"))), "c.d__1");
    }

    @Test
    public void nonOpenSearchBackendIsUntouched() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        // Solr keeps the legacy behaviour: default name, no explicit MAPPED_NAME parameter
        assertEquals(withBackend("solr", () -> add(s, s.key("foo"))), "foo");

        Mockito.verify(s.janusMgmt).addIndexKey(any(JanusGraphIndex.class), any(PropertyKey.class));
    }

    @Test
    public void collisionAddsExplicitMappedNameParameter() throws Exception {
        FakeMixedIndex fake = new FakeMixedIndex();
        Session        s    = new Session(fake);

        fake.register(mock(PropertyKey.class), "foo");

        withBackend("opensearch", () -> add(s, s.key("foo")));

        Parameter<?>[] registered = fake.params.get(fake.params.size() - 1);
        String         mapped     = null;

        for (Parameter<?> p : registered) {
            if (ParameterType.MAPPED_NAME.getName().equals(p.key())) {
                mapped = String.valueOf(p.value());
            }
        }

        assertEquals(mapped, "foo__1");
    }
}
