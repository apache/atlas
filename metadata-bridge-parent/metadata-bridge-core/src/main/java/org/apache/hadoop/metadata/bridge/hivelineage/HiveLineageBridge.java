package org.apache.hadoop.metadata.bridge.hivelineage;

import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.bridge.ABridge;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TypeSystem;

public class HiveLineageBridge extends ABridge {

  static final String LINEAGE_CLASS_TYPE = "HiveLineage";

  @Override
  public boolean defineBridgeTypes(TypeSystem ts) {
    try {
      HierarchicalTypeDefinition<ClassType> lineageClassTypeDef =
          new HierarchicalTypeDefinition<ClassType>(
              "ClassType",
              LINEAGE_CLASS_TYPE,
              null,
              new AttributeDefinition[] {
                  new AttributeDefinition("QUERY_ID", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("HIVE_ID", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("USER", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("QUERY_START_TIME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("QUERY_END_TIME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("QUERY", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("TABLE_NAME", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("TABLE_LOCATION", "STRING_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("SUCCESS", "BOOLEAN_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("FAILED", "BOOLEAN_TYPE", Multiplicity.REQUIRED, false, null),
                  new AttributeDefinition("EXECUTION_ENGINE", "STRING_TYPE", Multiplicity.REQUIRED, false, null)
                  });
      
      // TODO - assess these
      /*
       * Not sure what to do with these attributes - wouldn't tables and columns be linked to
       * Hive Structure instances?
       * 
      ArrayList<SourceTables> sourceTables;
      ArrayList<QueryColumns> queryColumns;
      ArrayList<WhereClause> whereClause;
      ArrayList<CreateColumns> createColumns;
      ArrayList<GroupBy> groupBy;
      ArrayList<GroupBy> orderBy;*/
      
      ts.defineClassType(lineageClassTypeDef);
      
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (MetadataException e) {
      e.printStackTrace();
    }
    
    return false;
  }
}
