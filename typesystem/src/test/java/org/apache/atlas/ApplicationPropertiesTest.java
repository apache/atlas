/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.configuration.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ApplicationPropertiesTest {

    @Test
    public void testVariables() throws Exception {
        Configuration properties = ApplicationProperties.get(ApplicationProperties.APPLICATION_PROPERTIES);

        //plain property without variables
        assertEquals(properties.getString("atlas.service"), "atlas");

        //property containing system property
        String data = "/var/data/" + System.getProperty("user.name") + "/atlas";
        assertEquals(properties.getString("atlas.data"), data);

        //property referencing other property
        assertEquals(properties.getString("atlas.graph.data"), data + "/graph");

        //invalid system property - not substituted
        assertEquals(properties.getString("atlas.db"), "${atlasdb}");
    }

    @Test
    //variable substitutions should work with subset configuration as well
    public void testSubset() throws Exception {
        Configuration configuration = ApplicationProperties.get(ApplicationProperties.APPLICATION_PROPERTIES);
        Configuration subConfiguration = configuration.subset("atlas");

        assertEquals(subConfiguration.getString("service"), "atlas");
        String data = "/var/data/" + System.getProperty("user.name") + "/atlas";
        assertEquals(subConfiguration.getString("data"), data);
        assertEquals(subConfiguration.getString("graph.data"), data + "/graph");
    }

    @Test
    public void testGetClass() throws Exception {
        //read from atlas-application.properties
        Class cls = ApplicationProperties.getClass("atlas.TypeSystem.impl", ApplicationProperties.class.getName(), TypeSystem.class);
        assertEquals(cls.getName(), TypeSystem.class.getName());

        //default value
        cls = ApplicationProperties.getClass("atlas.TypeSystem2.impl", TypeSystem.class.getName(), TypeSystem.class);
        assertEquals(cls.getName(), TypeSystem.class.getName());

        //incompatible assignTo class, should throw AtlasException
        try {
            cls = ApplicationProperties.getClass("atlas.TypeSystem.impl", ApplicationProperties.class.getName(), ApplicationProperties.class);
            Assert.fail(AtlasException.class.getSimpleName() + " was expected but none thrown.");
        }
        catch (AtlasException e) {
            // good
        }
    }
}
