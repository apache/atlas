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

package org.apache.metadata.falcon;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.DataTypes;
import org.apache.hadoop.metadata.typesystem.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.EnumValue;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.IDataType;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.StructTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.TraitType;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FalconTypeSystem {
    public static final Logger LOG = LoggerFactory.getLogger(FalconTypeSystem.class);
    public static final TypeSystem TYPE_SYSTEM = TypeSystem.getInstance();

    private List<StructTypeDefinition> structTypeDefinitions = new ArrayList<>();
    private List<HierarchicalTypeDefinition<TraitType>> traitTypeDefinitions = new ArrayList<>();

    private FalconTypeSystem() throws MetadataException {
        HierarchicalTypeDefinition<ClassType> cluster = defineCluster();
        //TODO define feed and process

        TYPE_SYSTEM.defineTypes(ImmutableList.copyOf(structTypeDefinitions), ImmutableList.copyOf(traitTypeDefinitions),
                ImmutableList.of(cluster));
    }

    public static FalconTypeSystem getInstance() throws MetadataException {
        if (INSTANCE == null) {
            synchronized (LOG) {
                if (INSTANCE == null) {
                    INSTANCE = new FalconTypeSystem();
                }
            }
        }
        return INSTANCE;
    }

    private HierarchicalTypeDefinition<ClassType> defineCluster() throws MetadataException {
        defineACL();
        defineClusterInterface();
        defineClusterLocation();
        defineTags();

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("acl", DefinedTypes.ACL.name(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("tags", DefinedTypes.TAG.name(), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("locations", TYPE_SYSTEM.defineMapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("interfaces", DefinedTypes.CLUSTER_INTERFACE.name(), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("properties", TYPE_SYSTEM.defineMapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.OPTIONAL, false, null),
        };
        HierarchicalTypeDefinition<ClassType> cluster =
                new HierarchicalTypeDefinition<>(ClassType.class, DefinedTypes.CLUSTER.name(),
                        ImmutableList.<String>of(), attributeDefinitions);
        LOG.debug("Created definition for " + DefinedTypes.CLUSTER.name());
        return cluster;
    }

    private HierarchicalTypeDefinition<TraitType> defineTags() {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("name", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("value", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null)
        };

        HierarchicalTypeDefinition<TraitType> traitType = new HierarchicalTypeDefinition<>(TraitType.class, DefinedTypes.TAG.name(), ImmutableList.<String>of(), attributeDefinitions);
        LOG.debug("Created definition for " + DefinedTypes.TAG.name());
        traitTypeDefinitions.add(traitType);
        return traitType;
    }

    private StructTypeDefinition defineClusterLocation() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("WORKING", 1),
                new EnumValue("STAGING", 2),
                new EnumValue("TEMP", 3),
        };

        LOG.debug("Created definition for " + DefinedTypes.CLUSTER_LOCATION_TYPE.name());
        EnumTypeDefinition locationType = new EnumTypeDefinition(DefinedTypes.CLUSTER_LOCATION_TYPE.name(), values);
        TYPE_SYSTEM.defineEnumType(locationType);

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("type", DefinedTypes.CLUSTER_LOCATION_TYPE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("path", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
        };
        LOG.debug("Created definition for " + DefinedTypes.CLUSTER_LOCATION.name());
        StructTypeDefinition location = new StructTypeDefinition(DefinedTypes.CLUSTER_LOCATION.name(), attributeDefinitions);
        structTypeDefinitions.add(location);
        return location;
    }

    private StructTypeDefinition defineClusterInterface() throws MetadataException {
        EnumValue values[] = {
                new EnumValue("READONLY", 1),
                new EnumValue("WRITE", 2),
                new EnumValue("EXECUTE", 3),
                new EnumValue("WORKFLOW", 4),
                new EnumValue("MESSAGING", 5),
                new EnumValue("REGISTRY", 6),
        };

        LOG.debug("Created definition for " + DefinedTypes.CLUSTER_INTERFACE_TYPE.name());
        EnumTypeDefinition interfaceType = new EnumTypeDefinition(DefinedTypes.CLUSTER_INTERFACE_TYPE.name(), values);
        TYPE_SYSTEM.defineEnumType(interfaceType);

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("type", DefinedTypes.CLUSTER_INTERFACE_TYPE.name(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("endpoint", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("version", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
        };
        LOG.debug("Created definition for " + DefinedTypes.CLUSTER_INTERFACE.name());
        StructTypeDefinition interfaceEntity = new StructTypeDefinition(DefinedTypes.CLUSTER_INTERFACE.name(), attributeDefinitions);
        structTypeDefinitions.add(interfaceEntity);
        return interfaceEntity;
    }

    public static enum DefinedTypes {
        ACL,
        TAG,

        CLUSTER,
        CLUSTER_INTERFACE,
        CLUSTER_INTERFACE_TYPE,
        CLUSTER_LOCATION,
        CLUSTER_LOCATION_TYPE
    }

    private StructTypeDefinition defineACL() {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition("owner", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("group", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("permission", DataTypes.STRING_TYPE.getName(),
                        Multiplicity.OPTIONAL, false, null),
        };
        LOG.debug("Created definition for " + DefinedTypes.ACL.name());
        StructTypeDefinition acl = new StructTypeDefinition(DefinedTypes.ACL.name(), attributeDefinitions);
        structTypeDefinitions.add(acl);
        return acl;
    }
}
