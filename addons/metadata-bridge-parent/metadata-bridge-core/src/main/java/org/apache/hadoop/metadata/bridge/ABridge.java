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

package org.apache.atlas.bridge;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.MetadataException;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public abstract class ABridge implements IBridge {

    protected static final Logger LOG = BridgeManager.LOG;
    protected ArrayList<Class<? extends AEntityBean>> typeBeanClasses = new ArrayList<Class<? extends AEntityBean>>();
    MetadataRepository repo;

    protected ABridge(MetadataRepository repo) {
        this.repo = repo;
    }

    protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(String name, ImmutableList<String> superTypes,
            AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
    }

    public ArrayList<Class<? extends AEntityBean>> getTypeBeanClasses() {
        return typeBeanClasses;
    }

    public AEntityBean get(String id) throws RepositoryException {
        // get from the system by id (?)
        ITypedReferenceableInstance ref = repo.getEntityDefinition(id);
        // turn into a HiveLineageBean
        try {
            Class<AEntityBean> c = getTypeBeanInListByName(ref.getTypeName());
            return this.convertFromITypedReferenceable(ref, getTypeBeanInListByName(ref.getTypeName()));
        } catch (BridgeException | InstantiationException | IllegalAccessException |
                IllegalArgumentException | InvocationTargetException | NoSuchMethodException |
                SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public String create(AEntityBean bean) throws MetadataException {

        ClassType type = TypeSystem.getInstance().getDataType(ClassType.class, bean.getClass().getSimpleName());
        ITypedReferenceableInstance refBean = null;
        try {
            refBean = type.convert(this.convertToReferencable(bean), Multiplicity.REQUIRED);
            String id = repo.createEntity(refBean);
            return id;
        } catch (IllegalArgumentException | IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        throw new MetadataException("Cannot create entity");

    }

    public Iterable<String> list() throws RepositoryException {
        List<String> returnList = null;
        for (Class c : typeBeanClasses) {
            List<String> inBetweenList = repo.getEntityList(c.getSimpleName());
            try {
                returnList.addAll(inBetweenList);
            } catch (NullPointerException e) {
                returnList = inBetweenList;
            }
        }
        return returnList;
    }

    protected final boolean containsType(String s) {
        for (Class c : typeBeanClasses) {
            if (c.getSimpleName().equals(s)) {
                return true;
            }
        }
        return false;
    }

    protected final Class<AEntityBean> getTypeBeanInListByName(String s) throws BridgeException {
        if (containsType(s)) {
            for (Class c : typeBeanClasses) {
                if (c.getSimpleName().equals(s)) {
                    return c;
                }
            }
        } else {
            throw new BridgeException("No EntityBean Definition Found");
        }
        throw new BridgeException("No EntityBean Definition Found");
    }

    protected final <T extends AEntityBean> Referenceable convertToReferencable(T o)
    throws IllegalArgumentException, IllegalAccessException {
        Referenceable selfAware = new Referenceable(o.getClass().getSimpleName());
        // TODO - support non-primitive types and deep inspection
        for (Field f : o.getClass().getFields()) {
            selfAware.set(f.getName(), f.get(o));
        }
        return selfAware;
    }

    protected final <T extends AEntityBean> T convertFromITypedReferenceable(ITypedReferenceableInstance instance,
            Class<? extends AEntityBean> c)
    throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
    NoSuchMethodException, SecurityException, BridgeException {
        if (!instance.getTypeName().equals(c.getSimpleName())) {
            throw new BridgeException("ReferenceableInstance type not the same as bean");
        }
        Object retObj = this.getClass().newInstance();
        for (Entry<String, AttributeInfo> e : instance.fieldMapping().fields.entrySet()) {
            try {

                String convertedName = e.getKey().substring(0, 1).toUpperCase() + e.getKey().substring(1);
                this.getClass().getMethod("set" + convertedName, Class.forName(e.getValue().dataType().getName()))
                        .invoke(this, instance.get(e.getKey()));
            } catch (MetadataException | ClassNotFoundException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return (T) retObj;
    }
}
