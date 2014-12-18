package org.apache.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.metadata.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClassTest extends BaseTest {


    @Before
    public void setup() throws MetadataException {
        super.setup();
    }

    /*
     * Class Hierarchy is:
     *   Department(name : String, employees : Array[Person])
     *   Person(name : String, department : Department, manager : Manager)
     *   Manager(subordinates : Array[Person]) extends Person
     *
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    @Test
    public void test1() throws MetadataException {

        TypeSystem ts = ms.getTypeSystem();

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department", ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees",
                        String.format("array<%s>", "Person"), Multiplicity.COLLECTION, true, "department")
        );
        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("department",
                        "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager",
                        "Manager", Multiplicity.OPTIONAL, false, "subordinates")
        );
        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager",
                ImmutableList.<String>of("Person"),
                new AttributeDefinition("subordinates",
                        String.format("array<%s>", "Person"), Multiplicity.COLLECTION, false, "manager")
        );

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef = createTraitTypeDef("SecurityClearance",
                ImmutableList.<String>of(),
                createRequiredAttrDef("level", DataTypes.INT_TYPE)
        );

        ts.defineTypes(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(securityClearanceTypeDef),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of(deptTypeDef, personTypeDef, managerTypeDef));

        Referenceable hrDept = new Referenceable("Department");
        Referenceable john = new Referenceable("Person");
        Referenceable jane = new Referenceable("Manager", "SecurityClearance");

        hrDept.set("name", "hr");
        john.set("name", "John");
        john.set("department", hrDept);
        jane.set("name", "Jane");
        jane.set("department", hrDept);

        john.set("manager", jane);

        hrDept.set("employees", ImmutableList.<Referenceable>of(john, jane));

        jane.set("subordinates", ImmutableList.<Referenceable>of(john));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);

        Assert.assertEquals(hrDept2.toString(), "{\n" +
                "\tid : (type: Department, id: <unassigned>)\n" +
                "\tname : \thr\n" +
                "\temployees : \t[{\n" +
                "\tid : (type: Person, id: <unassigned>)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : (type: Manager, id: <unassigned>)\n" +
                "}, {\n" +
                "\tid : (type: Manager, id: <unassigned>)\n" +
                "\tsubordinates : \t[{\n" +
                "\tid : (type: Person, id: <unassigned>)\n" +
                "\tname : \tJohn\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : (type: Manager, id: <unassigned>)\n" +
                "}]\n" +
                "\tname : \tJane\n" +
                "\tdepartment : (type: Department, id: <unassigned>)\n" +
                "\tmanager : <null>\n" +
                "\n" +
                "\tSecurityClearance : \t{\n" +
                "\t\tlevel : \t\t1\n" +
                "\t}}]\n" +
                "}");

    }
}
