package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.types.TypeSystem;
import org.junit.Before;
import org.junit.Test;

public class StorageTest extends BaseTest {

    @Before
    public void setup() throws MetadataException {
        super.setup();
    }

    @Test
    public void test1() throws MetadataException {

        TypeSystem ts = ms.getTypeSystem();

        defineDeptEmployeeTypes(ts);


        Referenceable hrDept = createDeptEg1(ts);

        ITypedReferenceableInstance hrDept2 = ms.getRepository().create(hrDept);

        //System.out.println(hrDept2);

    }

}