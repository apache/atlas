/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.notification.preprocessor.PreprocessorContext.PreprocessAction;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HivePreprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(HivePreprocessor.class);

    private static final String RELATIONSHIP_TYPE_HIVE_TABLE_COLUMNS        = "hive_table_columns";
    private static final String RELATIONSHIP_TYPE_HIVE_TABLE_PARTITION_KEYS = "hive_table_partitionkeys";
    private static final String RELATIONSHIP_TYPE_HIVE_TABLE_STORAGEDESC    = "hive_table_storagedesc";

    static class HiveTablePreprocessor extends EntityPreprocessor {
        public HiveTablePreprocessor() {
            super(TYPE_HIVE_TABLE);
        }

        @Override
        public void preprocess(AtlasEntity entity, PreprocessorContext context) {
            if (context.isIgnoredEntity(entity.getGuid())) {
                context.addToIgnoredEntities(entity); // so that this will be logged with typeName and qualifiedName
            } else {
                PreprocessAction action = context.getPreprocessActionForHiveTable(getQualifiedName(entity));

                if (action == PreprocessAction.IGNORE) {
                    context.addToIgnoredEntities(entity);

                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_SD));
                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_COLUMNS));
                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_PARTITION_KEYS));
                } else if (action == PreprocessAction.PRUNE) {
                    context.addToPrunedEntities(entity);

                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_SD));
                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_COLUMNS));
                    context.addToIgnoredEntities(entity.getAttribute(ATTRIBUTE_PARTITION_KEYS));

                    entity.setAttribute(ATTRIBUTE_SD, null);
                    entity.setAttribute(ATTRIBUTE_COLUMNS, null);
                    entity.setAttribute(ATTRIBUTE_PARTITION_KEYS, null);
                } else if (context.getHiveTypesRemoveOwnedRefAttrs()) {
                    context.removeRefAttributeAndRegisterToMove(entity, ATTRIBUTE_SD, RELATIONSHIP_TYPE_HIVE_TABLE_STORAGEDESC, ATTRIBUTE_TABLE);
                    context.removeRefAttributeAndRegisterToMove(entity, ATTRIBUTE_COLUMNS, RELATIONSHIP_TYPE_HIVE_TABLE_COLUMNS, ATTRIBUTE_TABLE);
                    context.removeRefAttributeAndRegisterToMove(entity, ATTRIBUTE_PARTITION_KEYS, RELATIONSHIP_TYPE_HIVE_TABLE_PARTITION_KEYS, ATTRIBUTE_TABLE);
                }
            }
        }
    }


    static class HiveColumnPreprocessor extends EntityPreprocessor {
        public HiveColumnPreprocessor() {
            super(TYPE_HIVE_COLUMN);
        }

        @Override
        public void preprocess(AtlasEntity entity, PreprocessorContext context) {
            if (!context.isIgnoredEntity(entity.getGuid())) {
                PreprocessAction action = context.getPreprocessActionForHiveTable(getHiveTableQualifiedName(getQualifiedName(entity)));

                if (action == PreprocessAction.IGNORE || action == PreprocessAction.PRUNE) {
                    context.addToIgnoredEntities(entity.getGuid());
                }
            }
        }

        public static String getHiveTableQualifiedName(String columnQualifiedName) {
            String dbTableName = null;
            String clusterName = null;

            int sepPos = columnQualifiedName.lastIndexOf(QNAME_SEP_CLUSTER_NAME);

            if (sepPos != -1 && columnQualifiedName.length() > (sepPos + 1)) {
                clusterName = columnQualifiedName.substring(sepPos + 1);
            }

            sepPos = columnQualifiedName.lastIndexOf(QNAME_SEP_ENTITY_NAME);

            if (sepPos != -1) {
                dbTableName = columnQualifiedName.substring(0, sepPos);
            }

            return clusterName != null ? (dbTableName + QNAME_SEP_CLUSTER_NAME + clusterName) : dbTableName;
        }
    }


    static class HiveStorageDescPreprocessor extends EntityPreprocessor {
        public HiveStorageDescPreprocessor() {
            super(TYPE_HIVE_STORAGEDESC);
        }

        @Override
        public void preprocess(AtlasEntity entity, PreprocessorContext context) {
            if (!context.isIgnoredEntity(entity.getGuid())) {
                PreprocessAction action = context.getPreprocessActionForHiveTable(getHiveTableQualifiedName(getQualifiedName(entity)));

                if (action == PreprocessAction.IGNORE || action == PreprocessAction.PRUNE) {
                    context.addToIgnoredEntities(entity.getGuid());
                }
            }
        }

        public static String getHiveTableQualifiedName(String sdQualifiedName) {
            int sepPos = sdQualifiedName.lastIndexOf(QNAME_SD_SUFFIX);

            return sepPos != -1 ? sdQualifiedName.substring(0, sepPos) : sdQualifiedName;
        }
    }


    static class HiveProcessPreprocessor extends EntityPreprocessor {
        public HiveProcessPreprocessor() {
            super(TYPE_HIVE_PROCESS);
        }

        public HiveProcessPreprocessor(String typeName) {
            super(typeName);
        }

        @Override
        public void preprocess(AtlasEntity entity, PreprocessorContext context) {
            if (context.isIgnoredEntity(entity.getGuid())) {
                context.addToIgnoredEntities(entity); // so that this will be logged with typeName and qualifiedName
            } else {
                Object inputs  = entity.getAttribute(ATTRIBUTE_INPUTS);
                Object outputs = entity.getAttribute(ATTRIBUTE_OUTPUTS);

                int inputsCount  = (inputs instanceof Collection) ? ((Collection) inputs).size() : 0;
                int outputsCount = (outputs instanceof Collection) ? ((Collection) outputs).size() : 0;

                removeIgnoredObjectIds(inputs, context);
                removeIgnoredObjectIds(outputs, context);

                boolean isInputsEmpty  = isEmpty(inputs);
                boolean isOutputsEmpty = isEmpty(outputs);

                // if inputs/outputs became empty due to removal of ignored entities, ignore the process entity as well
                if ((inputsCount > 0 && isInputsEmpty) || (outputsCount > 0 && isOutputsEmpty)) {
                    context.addToIgnoredEntities(entity);

                    // since the process entity is ignored, entities referenced by inputs/outputs of this process entity
                    // may not be processed by Atlas, if they are present in referredEntities. So, move them from
                    // 'referredEntities' to 'entities'. However, this is not necessary for hive_column entities,
                    // as these entities would be referenced by hive_table entities
                    if (!StringUtils.equals(entity.getTypeName(), TYPE_HIVE_COLUMN_LINEAGE)) {
                        if (!isInputsEmpty) {
                            for (Object obj : (Collection) inputs) {
                                String guid = context.getGuid(obj);

                                context.addToReferredEntitiesToMove(guid);
                            }
                        } else if (!isOutputsEmpty) {
                            for (Object obj : (Collection) outputs) {
                                String guid = context.getGuid(obj);

                                context.addToReferredEntitiesToMove(guid);
                            }
                        }
                    }
                }
            }
        }

        private void removeIgnoredObjectIds(Object obj, PreprocessorContext context) {
            if (obj == null || !(obj instanceof Collection)) {
                return;
            }

            Collection   objList  = (Collection) obj;
            List<Object> toRemove = null;

            for (Object objElem : objList) {
                boolean removeEntry = false;
                String  guid        = context.getGuid(objElem);

                if (guid != null) {
                    removeEntry = context.isIgnoredEntity(guid);

                    if (!removeEntry) { // perhaps entity hasn't been preprocessed yet
                        AtlasEntity entity = context.getEntity(guid);

                        if (entity != null) {
                            switch (entity.getTypeName()) {
                                case TYPE_HIVE_TABLE: {
                                    PreprocessAction action = context.getPreprocessActionForHiveTable(getQualifiedName(entity));

                                    removeEntry = (action == PreprocessAction.IGNORE);
                                }
                                break;

                                case TYPE_HIVE_COLUMN: {
                                    PreprocessAction action = context.getPreprocessActionForHiveTable(HiveColumnPreprocessor.getHiveTableQualifiedName(getQualifiedName(entity)));

                                    // if the table is ignored or pruned, remove the column
                                    removeEntry = (action == PreprocessAction.IGNORE || action == PreprocessAction.PRUNE);
                                }
                                break;
                            }
                        }
                    }
                } else {
                    String typeName = getTypeName(objElem);

                    if (typeName != null) {
                        switch (typeName) {
                            case TYPE_HIVE_TABLE: {
                                PreprocessAction action = context.getPreprocessActionForHiveTable(getQualifiedName(objElem));

                                removeEntry = (action == PreprocessAction.IGNORE);
                            }
                            break;

                            case TYPE_HIVE_COLUMN: {
                                PreprocessAction action = context.getPreprocessActionForHiveTable(HiveColumnPreprocessor.getHiveTableQualifiedName(getQualifiedName(objElem)));

                                // if the table is ignored or pruned, remove the column
                                removeEntry = (action == PreprocessAction.IGNORE || action == PreprocessAction.PRUNE);
                            }
                            break;
                        }
                    }
                }

                if (removeEntry) {
                    if (toRemove == null) {
                        toRemove = new ArrayList();
                    }

                    toRemove.add(objElem);
                }
            }

            if (toRemove != null) {
                objList.removeAll(toRemove);
            }
        }
    }

    static class HiveColumnLineageProcessPreprocessor extends HiveProcessPreprocessor {
        public HiveColumnLineageProcessPreprocessor() {
            super(TYPE_HIVE_COLUMN_LINEAGE);
        }
    }
}
