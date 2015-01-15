package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClassTest extends BaseTest {

    @Before
    public void setup() throws MetadataException {
        super.setup();
    }

    @Test
    public void test1() throws MetadataException {

        TypeSystem ts = getTypeSystem();

        defineDeptEmployeeTypes(ts);
        Referenceable hrDept = createDeptEg1(ts);
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
