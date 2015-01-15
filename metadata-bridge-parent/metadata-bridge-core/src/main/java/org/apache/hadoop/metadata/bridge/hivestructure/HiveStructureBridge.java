package org.apache.hadoop.metadata.bridge.hivestructure;

import java.util.ArrayList;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.bridge.Bridge;
import org.apache.hadoop.metadata.bridge.BridgeAssistant;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TypeSystem;


public class HiveStructureBridge extends BridgeAssistant implements Bridge{

	static final String DB_CLASS_TYPE = "HiveDatabase";
	static final String TB_CLASS_TYPE = "HiveTable";
	static final String FD_CLASS_TYPE = "HiveField";
	@Override
	public boolean defineBridgeTypes(TypeSystem ts) {
		ArrayList<HierarchicalTypeDefinition<?>> al = new ArrayList<HierarchicalTypeDefinition<?>>();
		
		try{
		HierarchicalTypeDefinition<ClassType> databaseClassTypeDef = new HierarchicalTypeDefinition<ClassType>("ClassType",DB_CLASS_TYPE, null,
			new AttributeDefinition[]{
		        new AttributeDefinition("DESC", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
		        new AttributeDefinition("DB_LOCATION_URI", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("OWNER_TYPE", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
				new AttributeDefinition("OWNER_NAME", "STRING_TYPE", Multiplicity.OPTIONAL, false, null)
		    }
        );
		
		HierarchicalTypeDefinition<ClassType> tableClassTypeDef = new HierarchicalTypeDefinition<ClassType>("ClassType",TB_CLASS_TYPE, null,
				new AttributeDefinition[]{
				new AttributeDefinition("CREATE_TIME", "LONG_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("LAST_ACCESS_TIME", "LONG_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("OWNER", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("TBL_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("TBL_TYPE", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("VIEW_EXPANDED_TEXT", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
				new AttributeDefinition("VIEW_ORIGINAL_TEXT", "STRING_TYPE", Multiplicity.OPTIONAL, false, null)
				}
		);
		
		HierarchicalTypeDefinition<ClassType> columnClassTypeDef = new HierarchicalTypeDefinition<ClassType>("ClassType",FD_CLASS_TYPE, null,
				new AttributeDefinition[]{
				new AttributeDefinition("COMMENT", "STRING_TYPE", Multiplicity.OPTIONAL, false, null),
				new AttributeDefinition("COLUMN_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
				new AttributeDefinition("TYPE_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null)
				}
		);
		
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		
		for (HierarchicalTypeDefinition htd : al){
			try {
				ts.defineClassType(htd);
			} catch (MetadataException e) {
				System.out.println(htd.hierarchicalMetaTypeName + "could not be added to the type system");
				e.printStackTrace();
			}
		}
		
		

		return false;
	}

}
