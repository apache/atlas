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

package org.apache.atlas.storm.hook;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.storm.model.StormDataTypes;
import org.testng.annotations.Test;

import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test
public class StormAtlasHookTest {

    @Test
    public void testStormRegistersHiveDataModelIfNotPresent() throws AtlasException, AtlasServiceException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        HiveDataModelGenerator dataModelGenerator = mock(HiveDataModelGenerator.class);
        AtlasServiceException atlasServiceException = mock(AtlasServiceException.class);
        when(atlasServiceException.getStatus()).thenReturn(ClientResponse.Status.NOT_FOUND);
        when(atlasClient.getType(HiveDataTypes.HIVE_PROCESS.getName())).thenThrow(atlasServiceException);
        String hiveModel = "{hive_model_as_json}";
        when(dataModelGenerator.getModelAsJson()).thenReturn(hiveModel);

        StormAtlasHook stormAtlasHook = new StormAtlasHook(atlasClient);
        stormAtlasHook.registerDataModel(dataModelGenerator);

        verify(atlasClient).createType(hiveModel);
    }

    @Test
    public void testStormRegistersStormModelIfNotPresent() throws AtlasServiceException, AtlasException {
        AtlasClient atlasClient = mock(AtlasClient.class);
        HiveDataModelGenerator dataModelGenerator = mock(HiveDataModelGenerator.class);
        when(atlasClient.getType(HiveDataTypes.HIVE_PROCESS.getName())).thenReturn("hive_process_definition");
        AtlasServiceException atlasServiceException = mock(AtlasServiceException.class);
        when(atlasServiceException.getStatus()).thenReturn(ClientResponse.Status.NOT_FOUND);
        when(atlasClient.getType(StormDataTypes.STORM_TOPOLOGY.getName())).thenThrow(atlasServiceException);

        StormAtlasHook stormAtlasHook = new StormAtlasHook(atlasClient);
        stormAtlasHook.registerDataModel(dataModelGenerator);

        verify(atlasClient).createType(contains("storm_topology"));
    }
}
