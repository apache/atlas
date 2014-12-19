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

package org.apache.hadoop.metadata.bridge.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.MetadataService;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.types.StructType;
import org.apache.thrift.TException;
/*
 * Initial pass at one time importer TODO - needs re-write
 */


public class HiveMetaImporter {
/*	
	private static HiveMetaStoreClient msc;
	private static MetadataService ms;
	
	public HiveMetaImporter(MetadataService ms){
	
		try {
			this.ms = ms;
			msc = new HiveMetaStoreClient(new HiveConf());
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static boolean fullImport(){
		return false;
	}
	
	public static boolean databaseImport() throws MetaException{
        StructType structType = null;
		try {
			structType = (StructType) ms.getTypeSystem().getDataType(HiveBridge.STRUCT_TYPE_DB);
		} catch (MetadataException e1) {
			e1.printStackTrace();
		}
		for(String dbName : msc.getAllDatabases()){
			try {
				Database db = msc.getDatabase(dbName);
				Struct s = new Struct(structType.getName());
				s.set("DESC", db.getDescription());
				s.set("DB_LOCATION_URI", db.getLocationUri());
				s.set("NAME", db.getName());
				if(db.isSetOwnerType()){s.set("OWNER_TYPE", db.getOwnerType());}
				if(db.isSetOwnerName()){s.set("OWNER_NAME", db.getOwnerName());}
			} catch (NoSuchObjectException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return true;
	}
	
	public static boolean tableImport(String dbName) throws MetaException{
		StructType structType = null;
		try {
			structType = (StructType) ms.getTypeSystem().getDataType(HiveBridge.STRUCT_TYPE_TB);
		} catch (MetadataException e1) {
			e1.printStackTrace();
		}
		for(String tbName : msc.getAllTables(dbName)){
			try {
				Table tb = msc.getTable(dbName, tbName);
				Struct s = new Struct(structType.getName());
				s.set("CREATE_TIME", tb.getCreateTime());
				s.set("LAST_ACCESS_TIME", tb.getLastAccessTime());
				s.set("OWNER", tb.getOwner());
				s.set("TBL_NAME", tb.getTableName());
				s.set("TBL_TYPE", tb.getTableType());
				if(tb.isSetViewExpandedText()){s.set("VIEW_EXPANDED_TEXT", tb.getViewExpandedText());}
				if(tb.isSetViewOriginalText()){s.set("VIEW_ORIGINAL_TEXT", tb.getViewOriginalText());}
			} catch (NoSuchObjectException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return true;
	}
	
	public static boolean fieldImport(String dbName, String tbName) throws MetaException{
		StructType structType = null;
		try {
			structType = (StructType) ms.getTypeSystem().getDataType(HiveBridge.STRUCT_TYPE_FD);
		} catch (MetadataException e1) {
			e1.printStackTrace();
		}
		try {
			for(FieldSchema fs : msc.getFields(dbName, tbName)){
				Struct s = new Struct(structType.getName());
				if(fs.isSetComment()){s.set("COMMENT", fs.getName());}
				s.set("COLUMN_NAME", fs.getName());
				s.set("TYPE_NAME", fs.getType());
				
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
	*/
}
