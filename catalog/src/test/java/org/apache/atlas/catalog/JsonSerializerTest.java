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

import org.testng.annotations.Test;

import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.*;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.testng.Assert.assertEquals;

/**
 * Unit tests for JsonSerializer.
 */
public class JsonSerializerTest {
    @Test
    public void testSerialize() throws Exception {
        UriInfo uriInfo = createStrictMock(UriInfo.class);
        URI uri = new URI("http://test.com:8080/");
        expect(uriInfo.getBaseUri()).andReturn(uri);

        replay(uriInfo);

        Collection<Map<String, Object>> resultMaps = new ArrayList<>();
        // result map 1
        ResourceComparator resourceComparator = new ResourceComparator();
        Map<String, Object> resultMap1 = new TreeMap<>(resourceComparator);
        resultMaps.add(resultMap1);

        resultMap1.put("prop1", "property 1 value");
        resultMap1.put("booleanProp", true);
        resultMap1.put("numberProp", 100);
        resultMap1.put("href", "v1/testResources/foo");

        ArrayList<String> listProp = new ArrayList<>();
        listProp.add("one");
        listProp.add("two");
        resultMap1.put("listProp", listProp);

        Map<String, Object> mapProp = new TreeMap<>(resourceComparator);
        mapProp.put("mapProp1", "mapProp1Value");
        ArrayList<String> mapListProp = new ArrayList<>();
        mapListProp.add("mapListOne");
        mapListProp.add("mapListTwo");
        mapProp.put("mapListProp", mapListProp);
        mapProp.put("href", "v1/testResources/foobar");
        resultMap1.put("mapProp", mapProp);

        // result map 2
        Map<String, Object> resultMap2 = new TreeMap<>(resourceComparator);
        resultMaps.add(resultMap2);

        resultMap2.put("nullProp", null);
        resultMap2.put("href", "v1/testResources/bar");

        ArrayList<Map<String, Object>> listProp2 = new ArrayList<>();
        listProp2.add(Collections.<String, Object>singletonMap("listMapProp", "listMapPropValue"));
        resultMap2.put("listProp", listProp2);

        Result result = new Result(resultMaps);

        JsonSerializer serializer = new JsonSerializer();
        String resultJson = serializer.serialize(result, uriInfo);

        assertEquals(resultJson, EXPECTED_JSON);
    }

    private static final String EXPECTED_JSON =
            "[\n" +
            "    {\n" +
            "        \"href\": \"http://test.com:8080/v1/testResources/foo\",\n" +
            "        \"booleanProp\": true,\n" +
            "        \"numberProp\": 100,\n" +
            "        \"prop1\": \"property 1 value\",\n" +
            "        \"listProp\": [\n" +
            "            \"one\",\n" +
            "            \"two\"\n" +
            "        ],\n" +
            "        \"mapProp\": {\n" +
            "            \"href\": \"http://test.com:8080/v1/testResources/foobar\",\n" +
            "            \"mapProp1\": \"mapProp1Value\",\n" +
            "            \"mapListProp\": [\n" +
            "                \"mapListOne\",\n" +
            "                \"mapListTwo\"\n" +
            "            ]\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"href\": \"http://test.com:8080/v1/testResources/bar\",\n" +
            "        \"nullProp\": null,\n" +
            "        \"listProp\": [\n" +
            "            {\n" +
            "                \"listMapProp\": \"listMapPropValue\"\n" +
            "            }\n" +
            "        ]\n" +
            "    }\n" +
            "]";

}
