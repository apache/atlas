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

import java.util.ArrayList;

import org.apache.hadoop.metadata.bridge.Bridge;
import org.apache.hadoop.metadata.bridge.BridgeLoad;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.bridge.Bridge;
import org.apache.hadoop.metadata.bridge.BridgeLoad;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;

public class HiveBridge implements Bridge {
	
	public static final String STRUCT_TYPE_DB = "HIVE_DB_STRUCT";
    public static final String STRUCT_TYPE_TB = "HIVE_TB_STRUCT";
    public static final String STRUCT_TYPE_FD = "HIVE_FD_STRUCT";
	
	public ArrayList<StructTypeDefinition> getStructTypeDefinitions() {
		ArrayList<StructTypeDefinition> stds = new ArrayList<StructTypeDefinition>();
		stds.add(new StructTypeDefinition(STRUCT_TYPE_DB,
		        //new AttributeDefinition("DB_ID", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
		        new AttributeDefinition[]{
			        new AttributeDefinition("DESC", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
			        new AttributeDefinition("DB_LOCATION_URI", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("OWNER_TYPE", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
					new AttributeDefinition("OWNER_NAME", "STRING_TYPE", Multiplicity.OPTIONAL, false, null)
			        }
				)
		);
		stds.add(new StructTypeDefinition(STRUCT_TYPE_TB, 
				//new AttributeDefinition("TBL_ID", "INT_TYPE", Multiplicity.REQUIRED, false, null),
				
				new AttributeDefinition[]{
					new AttributeDefinition("CREATE_TIME", "LONG_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("LAST_ACCESS_TIME", "LONG_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("OWNER", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("TBL_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("TBL_TYPE", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("VIEW_EXPANDED_TEXT", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
					new AttributeDefinition("VIEW_ORIGINAL_TEXT", "STRING_TYPE", Multiplicity.OPTIONAL, false, null)
					}
				)
		);
		stds.add(new StructTypeDefinition(STRUCT_TYPE_FD,
				//new AttributeDefinition("CD_ID", "INT_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition[]{
					new AttributeDefinition("COMMENT", "INT_TYPE", Multiplicity.OPTIONAL, false, null),
					new AttributeDefinition("COLUMN_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
					new AttributeDefinition("TYPE_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null)
					}
				)
		);
		return stds;
	}

	public boolean submitLoad(BridgeLoad load) {
		// TODO Auto-generated method stub
		return false;
	}
}