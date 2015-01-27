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

package org.apache.hadoop.metadata.web.resources;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.core.UriBuilder;

public abstract class BaseResourceIT {

    protected TypeSystem typeSystem;
    protected WebResource service;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem = TypeSystem.getInstance();
        typeSystem.reset();

        String baseUrl = "http://localhost:21000/";

        DefaultClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.resource(UriBuilder.fromUri(baseUrl).build());

        service = client.resource(UriBuilder.fromUri(baseUrl).build());
    }

    protected AttributeDefinition createUniqueRequiredAttrDef(String name,
                                                              IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, true, true, null);
    }

    protected AttributeDefinition createRequiredAttrDef(String name,
                                                        IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, null);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<TraitType> createTraitTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(TraitType.class, name, superTypes, attrDefs);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
    }
}
