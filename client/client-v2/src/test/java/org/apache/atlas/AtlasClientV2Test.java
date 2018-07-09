package org.apache.atlas;

import org.apache.commons.configuration.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AtlasClientV2Test {

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    AtlasClientV2 atlasClientV2;

    @BeforeClass
    public void testName() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        String[] atlasEndPoint = configuration.getStringArray(ATLAS_ENDPOINT);
        atlasClientV2 = new AtlasClientV2(atlasEndPoint, new String[]{"admin", "admin"});
    }

    @Test
    public void testIsServerReady() throws AtlasServiceException {
        Assert.assertTrue(atlasClientV2.isServerReady());
    }
}