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

package org.apache.atlas.bridge.hivestructure;

import org.apache.atlas.MetadataException;
import org.apache.atlas.repository.IRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;
/*
 * Initial pass at one time importer TODO - needs re-write
 */


public class HiveMetaImporter {

    private static HiveMetaStoreClient msc;
    private static IRepository repo;

    public HiveMetaImporter(IRepository repo) {

        try {
            this.repo = repo;
            msc = new HiveMetaStoreClient(new HiveConf());
            // TODO Get hive-site.conf from class path first
        } catch (MetaException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static boolean fullImport() {
        try {
            databasesImport();
            for (String dbName : msc.getAllDatabases()) {
                tablesImport(dbName);
                for (String tbName : msc.getAllTables(dbName)) {
                    fieldsImport(dbName, tbName);
                }
                return true;
            }
        } catch (MetaException me) {
            me.printStackTrace();
        } catch (RepositoryException re) {
            re.printStackTrace();
        }

        return false;
    }

    public static boolean databasesImport() throws MetaException, RepositoryException {
        ClassType classType = null;
        try {
            classType = TypeSystem.getInstance().getDataType(ClassType.class, HiveStructureBridge.DB_CLASS_TYPE);
        } catch (MetadataException e1) {
            e1.printStackTrace();
        }
        for (String dbName : msc.getAllDatabases()) {
            databaseImport(dbName);
        }
        return true;
    }

    public static boolean databaseImport(String dbName) throws MetaException, RepositoryException {
        try {
            Database db = msc.getDatabase(dbName);
            Referenceable dbRef = new Referenceable(HiveStructureBridge.DB_CLASS_TYPE);
            dbRef.set("DESC", db.getDescription());
            dbRef.set("DB_LOCATION_URI", db.getLocationUri());
            dbRef.set("NAME", db.getName());
            if (db.isSetOwnerType()) {
                dbRef.set("OWNER_TYPE", db.getOwnerType());
            }
            if (db.isSetOwnerName()) {
                dbRef.set("OWNER_NAME", db.getOwnerName());
            }

            repo.create(dbRef);
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    public static boolean tablesImport(String dbName) throws MetaException, RepositoryException {
        ClassType classType = null;
        try {
            classType = TypeSystem.getInstance().getDataType(ClassType.class, HiveStructureBridge.TB_CLASS_TYPE);
        } catch (MetadataException e1) {
            e1.printStackTrace();
        }
        for (String tbName : msc.getAllTables(dbName)) {
            tableImport(dbName, tbName);
        }
        return true;
    }

    public static boolean tableImport(String dbName, String tbName) throws MetaException, RepositoryException {
        try {
            Table tb = msc.getTable(dbName, tbName);
            Referenceable tbRef = new Referenceable(HiveStructureBridge.TB_CLASS_TYPE);
            tbRef.set("CREATE_TIME", tb.getCreateTime());
            tbRef.set("LAST_ACCESS_TIME", tb.getLastAccessTime());
            tbRef.set("OWNER", tb.getOwner());
            tbRef.set("TBL_NAME", tb.getTableName());
            tbRef.set("TBL_TYPE", tb.getTableType());
            if (tb.isSetViewExpandedText()) {
                tbRef.set("VIEW_EXPANDED_TEXT", tb.getViewExpandedText());
            }
            if (tb.isSetViewOriginalText()) {
                tbRef.set("VIEW_ORIGINAL_TEXT", tb.getViewOriginalText());
            }

            repo.create(tbRef);
        } catch (NoSuchObjectException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    public static boolean fieldsImport(String dbName, String tbName) throws MetaException, RepositoryException {
        ClassType classType = null;
        try {
            classType = TypeSystem.getInstance().getDataType(ClassType.class, HiveStructureBridge.FD_CLASS_TYPE);
        } catch (MetadataException e1) {
            e1.printStackTrace();
        }
        try {
            for (FieldSchema fs : msc.getFields(dbName, tbName)) {
                Referenceable fdRef = new Referenceable(HiveStructureBridge.FD_CLASS_TYPE);
                if (fs.isSetComment()) {
                    fdRef.set("COMMENT", fs.getName());
                }
                fdRef.set("COLUMN_NAME", fs.getName());
                fdRef.set("TYPE_NAME", fs.getType());

                repo.create(fdRef);
            }
        } catch (UnknownTableException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnknownDBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    public static boolean fieldImport(String dbName, String tbName, String fdName) throws MetaException {
        try {
            for (FieldSchema fs : msc.getFields(dbName, tbName)) {
                if (fs.getName().equals(fs)) {
                    Referenceable fdRef = new Referenceable(HiveStructureBridge.TB_CLASS_TYPE);
                    if (fs.isSetComment()) {
                        fdRef.set("COMMENT", fs.getName());
                    }
                    fdRef.set("COLUMN_NAME", fs.getName());
                    fdRef.set("TYPE_NAME", fs.getType());
                    //SaveObject to MS Backend
                    return true;
                }
            }
        } catch (UnknownTableException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnknownDBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

}
