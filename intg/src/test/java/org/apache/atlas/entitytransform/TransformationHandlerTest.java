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
package org.apache.atlas.entitytransform;

import org.apache.atlas.model.impexp.AttributeTransform;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.entitytransform.TransformationConstants.HDFS_PATH;

public class TransformationHandlerTest {
    @Test
    public void testHdfsClusterRenameHandler() {
        // Rename clusterName from cl1 to cl2
        AttributeTransform p1 = new AttributeTransform(Collections.singletonMap("hdfs_path.clusterName", "EQUALS: cl1"),
                                                       Collections.singletonMap("hdfs_path.clusterName", "SET: cl2"));

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p1));

        for (AtlasEntity hdfsPath : getHdfsPathEntities()) {
            String  qualifiedName = (String) hdfsPath.getAttribute("qualifiedName");
            boolean endsWithCl1   = qualifiedName.endsWith("@cl1");

            applyTransforms(hdfsPath, handlers);

            String transformedValue = (String) hdfsPath.getAttribute("qualifiedName");

            if (endsWithCl1) {
                Assert.assertTrue(transformedValue.endsWith("@cl2"), transformedValue + ": expected to end with @cl2");
            } else {
                Assert.assertEquals(qualifiedName, transformedValue, "not expected to change");
            }
        }
    }

    @Test
    public void testHdfsClusterNameToggleCaseHandler() {
        // Change clusterName to Upper case
        AttributeTransform p1 = new AttributeTransform(Collections.singletonMap("hdfs_path.clusterName", "EQUALS: cl1"),
                                                       Collections.singletonMap("hdfs_path.clusterName", "TO_UPPER:"));

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p1));

        List<AtlasEntity> hdfsPaths = getHdfsPathEntities();

        for (AtlasEntity hdfsPath : hdfsPaths) {
            String  qualifiedName = (String) hdfsPath.getAttribute("qualifiedName");
            boolean endsWithCl1   = qualifiedName.endsWith("@cl1");

            applyTransforms(hdfsPath, handlers);

            String transformedValue = (String) hdfsPath.getAttribute("qualifiedName");

            if (endsWithCl1) {
                Assert.assertTrue(transformedValue.endsWith("@CL1"), transformedValue + ": expected to end with @CL1");
            } else {
                Assert.assertEquals(qualifiedName, transformedValue, "not expected to change");
            }
        }

        // Change clusterName back to lower case
        AttributeTransform p2 = new AttributeTransform(Collections.singletonMap("hdfs_path.clusterName", "EQUALS: CL1"),
                                                       Collections.singletonMap("hdfs_path.clusterName", "TO_LOWER:"));

        handlers = initializeHandlers(Collections.singletonList(p2));

        for (AtlasEntity hdfsPath : hdfsPaths) {
            String  qualifiedName = (String) hdfsPath.getAttribute("qualifiedName");
            boolean endsWithCL1   = qualifiedName.endsWith("@CL1");

            applyTransforms(hdfsPath, handlers);

            String transformedValue = (String) hdfsPath.getAttribute("qualifiedName");

            if (endsWithCL1) {
                Assert.assertTrue(transformedValue.endsWith("@cl1"), transformedValue + ": expected to end with @cl1");
            } else {
                Assert.assertEquals(qualifiedName, transformedValue, "not expected to change");
            }
        }
    }

    @Test
    public void testHdfsPathNameReplacePrefixHandler() {
        // Prefix replace hdfs_path name from /aa/bb/ to /xx/yy/
        AttributeTransform p1 = new AttributeTransform(Collections.singletonMap("hdfs_path.name", "STARTS_WITH: /aa/bb/"),
                                                       Collections.singletonMap("hdfs_path.name", "REPLACE_PREFIX: = :/aa/bb/=/xx/yy/"));

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p1));

        for (AtlasEntity hdfsPath : getHdfsPathEntities()) {
            String  name              = (String) hdfsPath.getAttribute("name");
            boolean startsWith_aa_bb_ = name.startsWith("/aa/bb/");

            applyTransforms(hdfsPath, handlers);

            String transformedValue = (String) hdfsPath.getAttribute("name");

            if (startsWith_aa_bb_) {
                Assert.assertTrue(transformedValue.startsWith("/xx/yy/"), transformedValue + ": expected to start with /xx/yy/");
            } else {
                Assert.assertEquals(name, transformedValue, "not expected to change");
            }
        }
    }

    @Test
    public void testHiveDatabaseClusterRenameHandler() {
        // replace clusterName: from cl1 to cl1_backup
        AttributeTransform p1 = new AttributeTransform(Collections.singletonMap("hive_db.clusterName", "EQUALS: cl1"),
                                                       Collections.singletonMap("hive_db.clusterName", "SET: cl1_backup"));

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p1));

        for (AtlasEntity entity : getAllEntities()) {
            String  qualifiedName = (String) entity.getAttribute("qualifiedName");
            boolean isHdfsPath    = StringUtils.equals(entity.getTypeName(), HDFS_PATH);
            boolean endsWithCl1   = qualifiedName.endsWith("@cl1");
            boolean containsCl1   = qualifiedName.contains("@cl1"); // for stroage_desc

            applyTransforms(entity, handlers);

            String transformedValue = (String) entity.getAttribute("qualifiedName");

            if (!isHdfsPath && endsWithCl1) {
                Assert.assertTrue(transformedValue.endsWith("@cl1_backup"), transformedValue + ": expected to end with @cl1_backup");
            } else if (!isHdfsPath && containsCl1) {
                Assert.assertTrue(transformedValue.contains("@cl1_backup"), transformedValue + ": expected to contains @cl1_backup");
            } else {
                Assert.assertEquals(qualifiedName, transformedValue, "not expected to change");
            }
        }
    }

    @Test
    public void testHiveDatabaseNameRenameHandler() {
        // replace dbName: from hr to hr_backup
        AttributeTransform p = new AttributeTransform(Collections.singletonMap("hive_db.name", "EQUALS: hr"),
                                                      Collections.singletonMap("hive_db.name", "SET: hr_backup"));

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p));

        for (AtlasEntity entity : getAllEntities()) {
            String  qualifiedName   = (String) entity.getAttribute("qualifiedName");
            boolean startsWithHrDot = qualifiedName.startsWith("hr."); // for tables, columns
            boolean startsWithHrAt  = qualifiedName.startsWith("hr@"); // for databases

            applyTransforms(entity, handlers);

            if (startsWithHrDot) {
                Assert.assertTrue(((String) entity.getAttribute("qualifiedName")).startsWith("hr_backup."));
            } else if (startsWithHrAt) {
                Assert.assertTrue(((String) entity.getAttribute("qualifiedName")).startsWith("hr_backup@"));
            } else {
                Assert.assertEquals(qualifiedName, (String) entity.getAttribute("qualifiedName"), "not expected to change");
            }
        }
    }

    @Test
    public void testHiveTableNameRenameHandler() {
        // replace tableName: from hr.employees to hr.employees_backup
        AttributeTransform p = new AttributeTransform();
        p.addCondition("hive_db.name", "EQUALS: hr");
        p.addCondition("hive_table.name", "EQUALS: employees");
        p.addAction("hive_table.name", "SET: employees_backup");

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p));

        for (AtlasEntity entity : getAllEntities()) {
            String  qualifiedName            = (String) entity.getAttribute("qualifiedName");
            boolean startsWithHrEmployeesDot = qualifiedName.startsWith("hr.employees."); // for columns
            boolean startsWithHrEmployeesAt  = qualifiedName.startsWith("hr.employees@"); // for tables

            applyTransforms(entity, handlers);

            if (startsWithHrEmployeesDot) {
                Assert.assertTrue(((String) entity.getAttribute("qualifiedName")).startsWith("hr.employees_backup."));
            } else if (startsWithHrEmployeesAt) {
                Assert.assertTrue(((String) entity.getAttribute("qualifiedName")).startsWith("hr.employees_backup@"));
            } else {
                Assert.assertEquals(qualifiedName, (String) entity.getAttribute("qualifiedName"), "not expected to change");
            }
        }
    }

    @Test
    public void testHiveColumnNameRenameHandler() {
        // replace columnName: from hr.employees.age to hr.employees.age_backup
        AttributeTransform p = new AttributeTransform();
        p.addCondition("hive_db.name", "EQUALS: hr");
        p.addCondition("hive_table.name", "EQUALS: employees");
        p.addCondition("hive_column.name", "EQUALS: age");
        p.addAction("hive_column.name", "SET: age_backup");

        List<BaseEntityHandler> handlers = initializeHandlers(Collections.singletonList(p));

        for (AtlasEntity entity : getAllEntities()) {
            String  qualifiedName              = (String) entity.getAttribute("qualifiedName");
            boolean startsWithHrEmployeesAgeAt = qualifiedName.startsWith("hr.employees.age@");

            applyTransforms(entity, handlers);

            if (startsWithHrEmployeesAgeAt) {
                Assert.assertTrue(((String) entity.getAttribute("qualifiedName")).startsWith("hr.employees.age_backup@"));
            } else {
                Assert.assertEquals(qualifiedName, (String) entity.getAttribute("qualifiedName"), "not expected to change");
            }
        }
    }

    private List<BaseEntityHandler> initializeHandlers(List<AttributeTransform> params) {
        return BaseEntityHandler.createEntityHandlers(params);
    }

    private void applyTransforms(AtlasEntity entity, List<BaseEntityHandler> handlers) {
        for (BaseEntityHandler handler : handlers) {
            handler.transform(entity);
        }
    }

    final String[] clusterNames  = new String[] { "cl1", "prod" };
    final String[] databaseNames = new String[] { "hr", "sales", "engg" };
    final String[] tableNames    = new String[] { "employees", "products", "invoice" };
    final String[] columnNames   = new String[] { "name", "age", "dob" };

    private List<AtlasEntity> getHdfsPathEntities() {
        List<AtlasEntity> ret = new ArrayList<>();

        for (String clusterName : clusterNames) {
            ret.add(getHdfsPathEntity1(clusterName));
            ret.add(getHdfsPathEntity2(clusterName));
        }

        return ret;
    }

    private List<AtlasEntity> getAllEntities() {
        List<AtlasEntity> ret = new ArrayList<>();

        for (String clusterName : clusterNames) {
            ret.add(getHdfsPathEntity1(clusterName));
            ret.add(getHdfsPathEntity2(clusterName));

            for (String databaseName : databaseNames) {
                ret.add(getHiveDbEntity(clusterName, databaseName));

                for (String tableName : tableNames) {
                    ret.add(getHiveTableEntity(clusterName, databaseName, tableName));
                    ret.add(getHiveStorageDescriptorEntity(clusterName, databaseName, tableName));

                    for (String columnName : columnNames) {
                        ret.add(getHiveColumnEntity(clusterName, databaseName, tableName, columnName));
                    }
                }
            }
        }

        return ret;
    }

    private AtlasEntity getHdfsPathEntity1(String clusterName) {
        AtlasEntity entity = new AtlasEntity(HDFS_PATH);

        entity.setAttribute("name", "/aa/bb/employee");
        entity.setAttribute("path", "hdfs://localhost.localdomain:8020/aa/bb/employee");
        entity.setAttribute("qualifiedName", "hdfs://localhost.localdomain:8020/aa/bb/employee@" + clusterName);
        entity.setAttribute("clusterName", clusterName);
        entity.setAttribute("isSymlink", false);
        entity.setAttribute("modifiedTime", 0);
        entity.setAttribute("isFile", false);
        entity.setAttribute("numberOfReplicas", 0);
        entity.setAttribute("createTime", 0);
        entity.setAttribute("fileSize", 0);

        return entity;
    }

    private AtlasEntity getHdfsPathEntity2(String clusterName) {
        AtlasEntity entity = new AtlasEntity(HDFS_PATH);

        entity.setAttribute("name", "/cc/dd/employee");
        entity.setAttribute("path", "hdfs://localhost.localdomain:8020/cc/dd/employee");
        entity.setAttribute("qualifiedName", "hdfs://localhost.localdomain:8020/cc/dd/employee@" + clusterName);
        entity.setAttribute("clusterName", clusterName);
        entity.setAttribute("isSymlink", false);
        entity.setAttribute("modifiedTime", 0);
        entity.setAttribute("isFile", false);
        entity.setAttribute("numberOfReplicas", 0);
        entity.setAttribute("createTime", 0);
        entity.setAttribute("fileSize", 0);

        return entity;
    }

    private AtlasEntity getHiveDbEntity(String clusterName, String dbName) {
        AtlasEntity entity = new AtlasEntity(TransformationConstants.HIVE_DATABASE);

        entity.setAttribute("name", dbName);
        entity.setAttribute("qualifiedName", dbName + "@" + clusterName);
        entity.setAttribute("location", "hdfs://localhost.localdomain:8020/warehouse/tablespace/managed/hive/" + dbName + ".db");
        entity.setAttribute("clusterName", clusterName);
        entity.setAttribute("owner", "hive");
        entity.setAttribute("ownerType", "USER");

        return entity;
    }

    private AtlasEntity getHiveTableEntity(String clusterName, String dbName, String tableName) {
        AtlasEntity entity = new AtlasEntity(TransformationConstants.HIVE_TABLE);

        entity.setAttribute("name", tableName);
        entity.setAttribute("qualifiedName", dbName + "." + tableName + "@" + clusterName);
        entity.setAttribute("owner", "hive");
        entity.setAttribute("temporary", false);
        entity.setAttribute("lastAccessTime", "1535656355000");
        entity.setAttribute("tableType", "EXTERNAL_TABLE");
        entity.setAttribute("createTime", "1535656355000");
        entity.setAttribute("retention", 0);

        return entity;
    }

    private AtlasEntity getHiveStorageDescriptorEntity(String clusterName, String dbName, String tableName) {
        AtlasEntity entity = new AtlasEntity(TransformationConstants.HIVE_STORAGE_DESCRIPTOR);

        entity.setAttribute("qualifiedName", dbName + "." + tableName + "@" + clusterName + "_storage");
        entity.setAttribute("storedAsSubDirectories", false);
        entity.setAttribute("location", "hdfs://localhost.localdomain:8020/warehouse/tablespace/managed/hive/" + dbName + ".db" + "/" + tableName);
        entity.setAttribute("compressed", false);
        entity.setAttribute("inputFormat", "org.apache.hadoop.mapred.TextInputFormat");
        entity.setAttribute("outputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        entity.setAttribute("numBuckets", -1);

        return entity;
    }

    private AtlasEntity getHiveColumnEntity(String clusterName, String dbName, String tableName, String columnName) {
        AtlasEntity entity = new AtlasEntity(TransformationConstants.HIVE_COLUMN);

        entity.setAttribute("owner", "hive");
        entity.setAttribute("qualifiedName", dbName + "." + tableName + "." + columnName +"@" + clusterName);
        entity.setAttribute("name", columnName);
        entity.setAttribute("position", 1);
        entity.setAttribute("type", "string");

        return entity;
    }
}