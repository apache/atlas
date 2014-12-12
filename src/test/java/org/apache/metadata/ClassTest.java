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
     *   Department(name, employee : Array[Person])
     *   Person(name, Department, Manager)
     *   Manager(subordinate : Array[Person]) extends Person
     *
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    @Test
    public void test1() throws MetadataException {

        TypeSystem ts = ms.getTypeSystem();

        /*ClassTypeDefinition deptDef = createClassTypeDef("Department", ImmutableList.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createOptionalAttrDef("employee", ts.defineArrayType(DataTypes.INT_TYPE)))*/
    }
}
