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

package org.apache.hadoop.metadata.bridge.hivestructure;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.bridge.ABridge;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.typesystem.types.AttributeDefinition;
import org.apache.hadoop.metadata.typesystem.types.ClassType;
import org.apache.hadoop.metadata.typesystem.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.typesystem.types.Multiplicity;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;

import javax.inject.Inject;
import java.util.ArrayList;


public class HiveStructureBridge extends ABridge {


    static final String DB_CLASS_TYPE = "HiveDatabase";
    static final String TB_CLASS_TYPE = "HiveTable";
    static final String FD_CLASS_TYPE = "HiveField";

    @Inject
    protected HiveStructureBridge(MetadataRepository repo) {
        super(repo);
        // TODO Auto-generated constructor stub
    }

    public boolean defineBridgeTypes(TypeSystem ts) {
        ArrayList<HierarchicalTypeDefinition<?>> al
                = new ArrayList<HierarchicalTypeDefinition<?>>();
        // TODO
        //convert to helper methods
        // Add to arrayList

        try {
            HierarchicalTypeDefinition<ClassType> databaseClassTypeDef
                    = new HierarchicalTypeDefinition<ClassType>("ClassType", DB_CLASS_TYPE, null,
                    new AttributeDefinition[]{
                            new AttributeDefinition("DESC", "STRING_TYPE", Multiplicity.OPTIONAL,
                                    false, null),
                            new AttributeDefinition("DB_LOCATION_URI", "STRING_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("NAME", "STRING_TYPE", Multiplicity.REQUIRED,
                                    false, null),
                            new AttributeDefinition("OWNER_TYPE", "STRING_TYPE",
                                    Multiplicity.OPTIONAL, false, null),
                            new AttributeDefinition("OWNER_NAME", "STRING_TYPE",
                                    Multiplicity.OPTIONAL, false, null)
                    }
            );

            HierarchicalTypeDefinition<ClassType> tableClassTypeDef
                    = new HierarchicalTypeDefinition<ClassType>("ClassType", TB_CLASS_TYPE, null,
                    new AttributeDefinition[]{
                            new AttributeDefinition("CREATE_TIME", "LONG_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("LAST_ACCESS_TIME", "LONG_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("OWNER", "STRING_TYPE", Multiplicity.REQUIRED,
                                    false, null),
                            new AttributeDefinition("TBL_NAME", "STRING_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("TBL_TYPE", "STRING_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("VIEW_EXPANDED_TEXT", "STRING_TYPE",
                                    Multiplicity.OPTIONAL, false, null),
                            new AttributeDefinition("VIEW_ORIGINAL_TEXT", "STRING_TYPE",
                                    Multiplicity.OPTIONAL, false, null)
                    }
            );

            HierarchicalTypeDefinition<ClassType> columnClassTypeDef
                    = new HierarchicalTypeDefinition<ClassType>("ClassType", FD_CLASS_TYPE, null,
                    new AttributeDefinition[]{
                            new AttributeDefinition("COMMENT", "STRING_TYPE", Multiplicity.OPTIONAL,
                                    false, null),
                            new AttributeDefinition("COLUMN_NAME", "STRING_TYPE",
                                    Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition("TYPE_NAME", "STRING_TYPE",
                                    Multiplicity.REQUIRED, false, null)
                    }
            );

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        for (HierarchicalTypeDefinition htd : al) {
            try {
                ts.defineClassType(htd);
            } catch (MetadataException e) {
                System.out.println(
                        htd.hierarchicalMetaTypeName + "could not be added to the type system");
                e.printStackTrace();
            }
        }


        return false;
    }

}
