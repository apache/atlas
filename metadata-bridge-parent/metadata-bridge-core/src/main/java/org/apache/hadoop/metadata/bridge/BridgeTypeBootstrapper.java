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

package org.apache.hadoop.metadata.bridge;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

@Singleton
public class BridgeTypeBootstrapper {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeTypeBootstrapper.class);
    private final Map<Class, IBridge> bridges;
    private boolean isSetup = false;

    @Inject
    BridgeTypeBootstrapper(Map<Class, IBridge> bridges)
    throws MetadataException {
        this.bridges = bridges;
    }

    public final static HierarchicalTypeDefinition<ClassType>
    convertEntityBeanToClassTypeDefinition(
            Class<? extends AEntityBean> class1) {
        ArrayList<AttributeDefinition> attDefAL = new ArrayList<AttributeDefinition>();
        for (Field f : class1.getFields()) {
            try {
                attDefAL.add(BridgeTypeBootstrapper.convertFieldtoAttributeDefiniton(f));
            } catch (MetadataException e) {
                BridgeManager.LOG.error("Class " + class1.getName()
                        + " cannot be converted to TypeDefinition");
                e.printStackTrace();
            }
        }

        HierarchicalTypeDefinition<ClassType> typeDef = new HierarchicalTypeDefinition<>(
                ClassType.class, class1.getSimpleName(), null,
                (AttributeDefinition[]) attDefAL
                        .toArray(new AttributeDefinition[0]));

        return typeDef;
    }

    public final static AttributeDefinition convertFieldtoAttributeDefiniton(
            Field f) throws MetadataException {

        return new AttributeDefinition(f.getName(),
                f.getType().getSimpleName().toLowerCase(), Multiplicity.REQUIRED, false, null);
    }

    public synchronized boolean bootstrap() throws MetadataException {
        if (isSetup)
            return false;
        else {
            LOG.info("Bootstrapping types");
            _bootstrap();
            isSetup = true;
            LOG.info("Bootstrapping complete");
            return true;
        }
    }

    private void _bootstrap() throws MetadataException {
        TypeSystem ts = TypeSystem.getInstance();
        for (IBridge bridge : bridges.values()) {
            LOG.info("Registering bridge, %s", bridge.getClass().getSimpleName());
            loadTypes(bridge, ts);
        }
    }

    private final boolean loadTypes(IBridge bridge, TypeSystem ts)
    throws MetadataException {
        for (Class<? extends AEntityBean> clazz : bridge.getTypeBeanClasses()) {
            LOG.info("Registering %s", clazz.getSimpleName());
            ts.defineClassType(BridgeTypeBootstrapper
                    .convertEntityBeanToClassTypeDefinition(clazz));
        }
        return false;
    }
}
