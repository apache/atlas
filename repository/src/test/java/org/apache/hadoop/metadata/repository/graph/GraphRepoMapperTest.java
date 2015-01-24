package org.apache.hadoop.metadata.repository.graph;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.Referenceable;
import org.apache.hadoop.metadata.RepositoryModuleBaseTest;
import org.apache.hadoop.metadata.Struct;
import org.apache.hadoop.metadata.types.AttributeDefinition;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.DataTypes;
import org.apache.hadoop.metadata.types.EnumTypeDefinition;
import org.apache.hadoop.metadata.types.EnumValue;
import org.apache.hadoop.metadata.types.HierarchicalTypeDefinition;
import org.apache.hadoop.metadata.types.IDataType;
import org.apache.hadoop.metadata.types.Multiplicity;
import org.apache.hadoop.metadata.types.StructTypeDefinition;
import org.apache.hadoop.metadata.types.TraitType;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphRepoMapperTest extends RepositoryModuleBaseTest {

    private static final String DATABASE_TYPE = "hive_database";
    private static final String DATABASE_NAME = "foo";
    private static final String TABLE_TYPE = "hive_table";
    private static final String TABLE_NAME = "bar";

    private TitanGraphService titanGraphService;
    private GraphBackedMetadataRepository repositoryService;
    private TypeSystem typeSystem;

    @BeforeClass
    public void setUp() throws Exception {
        titanGraphService = super.injector.getInstance(TitanGraphService.class);
        titanGraphService.start();

        repositoryService = super.injector.getInstance(GraphBackedMetadataRepository.class);
        repositoryService.start();

        typeSystem = TypeSystem.getInstance();

        createHiveTypes();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
        System.out.println("databaseInstance = " + databaseInstance);

        ClassType dbType = typeSystem.getDataType(ClassType.class, DATABASE_TYPE);
        ITypedReferenceableInstance db = dbType.convert(databaseInstance, Multiplicity.REQUIRED);
        System.out.println("db = " + db);

        String dbGUID = repositoryService.createEntity(db, DATABASE_TYPE);
        System.out.println("added db = " + dbGUID);

        Referenceable dbInstance = new Referenceable(
                dbGUID, DATABASE_TYPE, databaseInstance.getValuesMap());

        ITypedReferenceableInstance table = createHiveTableInstance(dbInstance);
        String tableGUID = repositoryService.createEntity(table, TABLE_TYPE);
        System.out.println("added table = " + tableGUID);

        dumpGraph();
    }

    private void dumpGraph() {
        TitanGraph graph = titanGraphService.getTitanGraph();
        System.out.println("*******************Graph Dump****************************");
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
        System.out.println("*******************Graph Dump****************************");
    }

    private void createHiveTypes() throws Exception {
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition =
                new StructTypeDefinition("serdeType",
                        new AttributeDefinition[] {
                                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                                createRequiredAttrDef("serde", DataTypes.STRING_TYPE)
                        });

        EnumValue values[] = {
                new EnumValue("MANAGED", 1),
                new EnumValue("EXTERNAL", 2),
        };

        EnumTypeDefinition enumTypeDefinition =  new EnumTypeDefinition("tableType", values);
        typeSystem.defineEnumType(enumTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE,
                        ImmutableList.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        new AttributeDefinition("tableType", "tableType",
                                Multiplicity.REQUIRED, false, null),
/*
                        new AttributeDefinition("columns",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()),
                                Multiplicity.COLLECTION, false, null),
*/
                        new AttributeDefinition("serde1",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("serde2",
                                "serdeType", Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("database",
                                DATABASE_TYPE, Multiplicity.REQUIRED, true, null));

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef("classification",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        typeSystem.defineTypes(
                ImmutableList.of(structTypeDefinition),
                ImmutableList.of(classificationTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));
    }

    private ITypedReferenceableInstance createHiveTableInstance(
            Referenceable databaseInstance) throws Exception {
/*
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", DATABASE_NAME);
        databaseInstance.set("description", "foo database");
*/

        Referenceable tableInstance = new Referenceable(TABLE_TYPE, "classification");
        tableInstance.set("name", TABLE_NAME);
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("tableType", 1); // enum
        tableInstance.set("database", databaseInstance);

        Struct traitInstance = (Struct) tableInstance.getTrait("classification");
        traitInstance.set("tag", "foundation_etl");

        Struct serde1Instance = new Struct("serdeType");
        serde1Instance.set("name", "serde1");
        serde1Instance.set("serde", "serde1");
        tableInstance.set("serde1", serde1Instance);

        Struct serde2Instance = new Struct("serdeType");
        serde2Instance.set("name", "serde2");
        serde2Instance.set("serde", "serde2");
        tableInstance.set("serde2", serde2Instance);

        ClassType tableType = typeSystem.getDataType(ClassType.class, TABLE_TYPE);
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }

    protected AttributeDefinition createUniqueRequiredAttrDef(String name,
                                                              IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, true, true, null);
    }

    protected AttributeDefinition createRequiredAttrDef(String name,
                                                        IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(),
                Multiplicity.REQUIRED, false, null);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<TraitType> createTraitTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(TraitType.class, name, superTypes, attrDefs);
    }

    @SuppressWarnings("unchecked")
    protected HierarchicalTypeDefinition<ClassType> createClassTypeDef(
            String name, ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition(ClassType.class, name, superTypes, attrDefs);
    }
}
