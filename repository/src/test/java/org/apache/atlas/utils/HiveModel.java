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

package org.apache.atlas.utils;

import org.apache.atlas.TestUtils;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Allows easy creation of entities for classes in the hive test model.
 *
 */
public class HiveModel {

    public static class StructInstance {

        public String getTypeName() {
            return getClass().getSimpleName();
        }

        public Struct toStruct() throws Exception {

            Struct entity = new Struct(getTypeName());
            addDeclaredFields(getClass(), entity);
            return entity;
        }

        protected void addDeclaredFields(Class clazz, Struct r) throws Exception {
            
            for (Field f : clazz.getDeclaredFields()) {
                
                if (Modifier.isTransient(f.getModifiers())) {
                    continue;
                }
                String fieldName = f.getName();

                f.setAccessible(true);
                Object value = f.get(this);
                
                if (value instanceof List) {
                    
                    List listValue = (List) value;
                    List toSet = new ArrayList(listValue.size());
                    for (Object listItem : listValue) {
                        Object toAdd = null;
                        toAdd = convertValue(listItem);
                        toSet.add(toAdd);
                    }
                    r.set(fieldName, toSet);
                } else {
                    
                    Object converted = convertValue(value);
                    r.set(fieldName, converted);
                }
            }
            
            if (clazz != StructInstance.class) {
                addDeclaredFields(clazz.getSuperclass(), r);
            }
        }

        private Object convertValue(Object toConvert) throws Exception {

            if (toConvert instanceof ClassInstance) {
                return ((ClassInstance) toConvert).toReferenceable();
            }
            if (toConvert instanceof StructInstance) {
                return ((StructInstance) toConvert).toStruct();
            } else {
                return toConvert;
            }
        }
    }

    public static class ClassInstance<T> extends StructInstance {

        private transient final Id guid;
        private transient List<String> traits = new ArrayList();

        public T withTrait(String name) {
            traits.add(name);
            return getInstance();
        }

        public T withTraits(List<String> names) {
            traits.addAll(names);
            return getInstance();
        }

        public T getInstance() {
            return (T) this;
        }

        public ClassInstance() {
            guid = new Id(getTypeName());
        }

        public Referenceable toReferenceable() throws Exception {

            String[] traitArray = new String[traits.size()];
            traitArray = traits.toArray(traitArray);
            Referenceable entity = new Referenceable(getTypeName(), traitArray);
            entity.replaceWithNewId(guid);
            addDeclaredFields(getClass(), entity);

            return entity;
        }

        public List<ITypedReferenceableInstance> getTypedReferencebles() throws Exception {

            List<ITypedReferenceableInstance> result = new ArrayList();
            for (ClassInstance containedInstance : getAllInstances()) {
                Referenceable entity = containedInstance.toReferenceable();
                ClassType type = TypeSystem.getInstance().getDataType(ClassType.class, entity.getTypeName());
                ITypedReferenceableInstance converted = type.convert(entity, Multiplicity.REQUIRED);
                result.add(converted);
            }
            return result;
        }

        protected List<ClassInstance> getAllInstances() {

            return (List) Collections.singletonList(this);
        }

        public Id getId() {
            return guid;
        }
    }

    public static class NamedInstance<T> extends ClassInstance<T> {
        
        private final String name;

        public NamedInstance(String name) {
            super();
            this.name = name;
        }
    }

    public static class HiveOrder extends StructInstance {
        
        private String col;
        private int order;

        public HiveOrder(String col, int order) {
            super();
            this.col = col;
            this.order = order;
        }

    }

    public static class DB extends NamedInstance<DB> {
        
        private String owner;
        private int createTime;
        private String clusterName;

        public DB(String name, String owner, int createTime, String clusterName) {
            super(name);
            this.owner = owner;
            this.createTime = createTime;
            this.clusterName = clusterName;
        }
    }

    public static class StorageDescriptor extends ClassInstance<StorageDescriptor> {
        
        private String inputFormat;
        private String outputFormat;
        private List<HiveOrder> sortCols;

        public StorageDescriptor(String inputFormat, String outputFormat, List<HiveOrder> sortCols) {
            super();
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.sortCols = sortCols;
        }
    }

    public static class Column extends NamedInstance<Column> {

        private String type;
        private StorageDescriptor sd;

        public Column(String name, String type) {
            super(name);
            this.type = type;
        }

        public void setStorageDescriptor(StorageDescriptor sd) {
            this.sd = sd;
        }
    }

    public static class Table extends NamedInstance<Table> {
        
        private DB db;
        private Date created;
        private StorageDescriptor sd;
        private transient List<Column> colDefs;

        public Table(String name, DB db, StorageDescriptor sd, List<Column> colDefs) {
            this(name, db, sd, new Date(TestUtils.TEST_DATE_IN_LONG), colDefs);
        }

        public Table(String name, DB db, StorageDescriptor sd, Date created, List<Column> colDefs) {
            
            super(name);
            this.colDefs = colDefs;
            this.db = db;
            this.sd = sd;
            this.created = created;
            for (Column col : colDefs) {
                col.setStorageDescriptor(sd);
            }
        }

        public List<Column> getColumns() {
            return colDefs;
        }

        @Override
        protected List<ClassInstance> getAllInstances() {
            
            List<ClassInstance> result = new ArrayList(colDefs.size() + 2);
            result.add(sd);
            result.addAll(colDefs);
            result.add(this);
            return result;
        }
    }

    public static class Partition extends ClassInstance<Partition> {
        
        private List<String> values;
        private Table table;

        public Partition(List<String> values, Table table) {
            
            super();
            this.values = values;
            this.table = table;
        }

    }

    public static class LoadProcess extends NamedInstance<LoadProcess> {

        private List<Table> inputTables;
        private Table outputTable;

        public LoadProcess(String name, List<Table> inputTables, Table outputTable) {
            super(name);
            this.inputTables = inputTables;
            this.outputTable = outputTable;
        }

    }

    public static class View extends NamedInstance<View> {

        private DB db;
        private List<Table> inputTables;

        public View(String name, DB db, List<Table> inputTables) {
            super(name);
            this.db = db;
            this.inputTables = inputTables;
        }

    }

}
