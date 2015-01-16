package org.apache.hadoop.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.json.Serialization$;
import org.apache.hadoop.metadata.json.TypesSerialization;
import org.apache.hadoop.metadata.json.TypesSerialization$;
import org.apache.hadoop.metadata.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VenkateshTest extends BaseTest {

    protected List<HierarchicalTypeDefinition> createHiveTypes(TypeSystem typeSystem) throws MetadataException {
        ArrayList<HierarchicalTypeDefinition> typeDefinitions = new ArrayList<>();

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef("hive_database",
                        ImmutableList.<String>of(),
                        createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));
        typeDefinitions.add(databaseTypeDefinition);

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition = createClassTypeDef(
                "hive_table",
                ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                new AttributeDefinition("hive_database",
                        "hive_database", Multiplicity.REQUIRED, false, "hive_database"));
        typeDefinitions.add(tableTypeDefinition);

        HierarchicalTypeDefinition<TraitType> fetlTypeDefinition = createTraitTypeDef(
                "hive_fetl",
                ImmutableList.<String>of(),
                createRequiredAttrDef("level", DataTypes.INT_TYPE));
        typeDefinitions.add(fetlTypeDefinition);

        typeSystem.defineTypes(
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(fetlTypeDefinition),
                ImmutableList.of(databaseTypeDefinition, tableTypeDefinition));

        return typeDefinitions;
    }

    protected ITypedReferenceableInstance createHiveTableInstance(TypeSystem typeSystem) throws MetadataException {
        Referenceable databaseInstance = new Referenceable("hive_database");
        databaseInstance.set("name", "hive_database");
        databaseInstance.set("description", "foo database");

        Referenceable tableInstance = new Referenceable("hive_table", "hive_fetl");
        tableInstance.set("name", "t1");
        tableInstance.set("description", "bar table");
        tableInstance.set("type", "managed");
        tableInstance.set("hive_database", databaseInstance);

        Struct traitInstance = (Struct) tableInstance.getTrait("hive_fetl");
        traitInstance.set("level", 1);

        tableInstance.set("hive_fetl", traitInstance);

        ClassType tableType = typeSystem.getDataType(ClassType.class, "hive_table");
        return tableType.convert(tableInstance, Multiplicity.REQUIRED);
    }

    @Test
    public void testType() throws MetadataException {

        TypeSystem ts = getTypeSystem();

        createHiveTypes(ts);

        String jsonStr = TypesSerialization$.MODULE$.toJson(ts, ImmutableList.of("hive_database", "hive_table"));
        System.out.println(jsonStr);

        TypesDef typesDef1 = TypesSerialization$.MODULE$.fromJson(jsonStr);
        System.out.println(typesDef1);

        ts.reset();
        ts.defineTypes(typesDef1);
        jsonStr = TypesSerialization$.MODULE$.toJson(ts, ImmutableList.of("hive_database", "hive_table"));
        System.out.println(jsonStr);

    }

    @Test
    public void testInstance() throws MetadataException {

        TypeSystem ts = getTypeSystem();

        createHiveTypes(ts);

        ITypedReferenceableInstance i = createHiveTableInstance(getTypeSystem());

        String jsonStr = Serialization$.MODULE$.toJson(i);
        System.out.println(jsonStr);

        i = Serialization$.MODULE$.fromJson(jsonStr);
        System.out.println(i);
    }
}
