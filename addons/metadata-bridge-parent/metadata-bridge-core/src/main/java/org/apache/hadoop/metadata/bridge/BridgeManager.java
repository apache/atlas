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

//TODO - Create Index Annotation Framework for BeanConverter
//TODO - Enhance Bean Conversion to handled nested objects
//TODO - Enhance Bean COnversion to handle Collections

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;


public class BridgeManager {

    public static final Logger LOG = LoggerFactory.getLogger("BridgeLogger");
    private final static String bridgeFileDefault = "bridge-manager.properties";
    TypeSystem ts;
    MetadataRepository rs;
    ArrayList<ABridge> activeBridges;

    @Inject
    BridgeManager(MetadataRepository rs)
    throws ConfigurationException, ClassNotFoundException, InstantiationException,
    IllegalAccessException, IllegalArgumentException, InvocationTargetException,
    NoSuchMethodException, SecurityException {
        this.ts = TypeSystem.getInstance();
        this.rs = rs;
        if (System.getProperty("bridgeManager.propsFile") != null &&
                System.getProperty("bridgeManager.propsFile").length() != 0) {
            setActiveBridges(System.getProperty("bridgeManager.propsFile"));
        } else {
            setActiveBridges(bridgeFileDefault);
        }

        for (ABridge bridge : activeBridges) {
            try {
                this.loadTypes(bridge, ts);
            } catch (MetadataException e) {
                BridgeManager.LOG.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }

    }

    public final static HierarchicalTypeDefinition<ClassType>
    convertEntityBeanToClassTypeDefinition(
            Class<? extends AEntityBean> class1) {
        ArrayList<AttributeDefinition> attDefAL = new ArrayList<AttributeDefinition>();
        for (Field f : class1.getFields()) {
            try {
                attDefAL.add(BridgeManager.convertFieldtoAttributeDefiniton(f));
            } catch (MetadataException e) {
                BridgeManager.LOG.error("Class " + class1.getName() +
                        " cannot be converted to TypeDefinition");
                e.printStackTrace();
            }
        }

        HierarchicalTypeDefinition<ClassType> typeDef = new HierarchicalTypeDefinition<>(
                ClassType.class, class1.getSimpleName(),
                null, (AttributeDefinition[]) attDefAL.toArray(new AttributeDefinition[0]));

        return typeDef;
    }

    public final static AttributeDefinition convertFieldtoAttributeDefiniton(Field f)
    throws MetadataException {

        return new AttributeDefinition(f.getName(), f.getType().getSimpleName(),
                Multiplicity.REQUIRED, false, null);
    }

    public ArrayList<ABridge> getActiveBridges() {
        return this.activeBridges;
    }

    private void setActiveBridges(String bridgePropFileName) {
        if (bridgePropFileName == null || bridgePropFileName.isEmpty()) {
            bridgePropFileName = BridgeManager.bridgeFileDefault;
        }
        ArrayList<ABridge> aBList = new ArrayList<ABridge>();

        PropertiesConfiguration config = new PropertiesConfiguration();

        try {
            BridgeManager.LOG.info("Loading : Active Bridge List");
            config.load(bridgePropFileName);
            String[] activeBridgeList = ((String) config.getProperty("BridgeManager.activeBridges"))
                    .split(",");
            BridgeManager.LOG.info("Loaded : Active Bridge List");
            BridgeManager.LOG.info("First Loaded :" + activeBridgeList[0]);

            for (String s : activeBridgeList) {
                Class<?> bridgeCls = (Class<?>) Class.forName(s);
                if (ABridge.class.isAssignableFrom(bridgeCls)) {
                    System.out.println(s + " is able to be instaciated");
                    aBList.add((ABridge) bridgeCls.getConstructor(MetadataRepository.class)
                            .newInstance(rs));
                }
            }

        } catch (InstantiationException | ConfigurationException | IllegalAccessException |
                IllegalArgumentException | InvocationTargetException | NoSuchMethodException |
                SecurityException | ClassNotFoundException e) {
            BridgeManager.LOG.error(e.getMessage(), e);
            e.printStackTrace();
        }
        this.activeBridges = aBList;


    }

    private final boolean loadTypes(ABridge bridge, TypeSystem ts) throws MetadataException {
        for (Class<? extends AEntityBean> clazz : bridge.getTypeBeanClasses()) {
            ts.defineClassType(BridgeManager.convertEntityBeanToClassTypeDefinition(clazz));
        }
        return false;


    }

}
