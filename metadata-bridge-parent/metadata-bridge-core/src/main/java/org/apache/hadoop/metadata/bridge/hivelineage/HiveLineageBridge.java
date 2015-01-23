package org.apache.hadoop.metadata.bridge.hivelineage;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.bridge.Bridge;
import org.apache.hadoop.metadata.bridge.hivelineage.hook.HiveLineageBean;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.TypeSystem;

import com.google.common.collect.ImmutableList;

public class HiveLineageBridge implements Bridge {

  static final String LINEAGE_CLASS_TYPE = "HiveLineage";
  
  private final MetadataRepository repo;
  
  @Inject
  HiveLineageBridge(MetadataRepository repo) {
	  this.repo = repo;
  }

  @Override
  public boolean defineBridgeTypes(TypeSystem ts) {
	  // new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
	  
    try {
      HierarchicalTypeDefinition<ClassType> lineageClassTypeDef =
          new HierarchicalTypeDefinition(
              ClassType.class,
              LINEAGE_CLASS_TYPE,
              ImmutableList.<String>of(),
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
      
    } catch (MetadataException e) {
      e.printStackTrace();
    }
    
    return false;
  }
  
  public HiveLineageBean get(String id) throws RepositoryException {
	  // get from the system by id (?)
	  ITypedReferenceableInstance ref = repo.getEntityDefinition(id);
	  // turn into a HiveLineageBean
	  HiveLineageBean hlb = null;
	  return hlb;
  }
  
  public String create(HiveLineageBean bean) throws RepositoryException {
	  // turn the bean into something usable by the repo
	  // ???
	  
	  // put bean into the repo (?)
	  String id = repo.createEntity(null, LINEAGE_CLASS_TYPE);
	  // return id of the entity OR the new full entity
	  return id;
  }
  
  public Iterable<String> list() throws RepositoryException {
	  List<String> lineage = repo.getEntityList(LINEAGE_CLASS_TYPE);
	  // can stub out a Map() wrapper that iterates over the list of GUIDS
	  // replacing them with the results of invocations to the get(id) method
	  // other avenue: implement this differently in the repo
	  return lineage;
  }
}
