package org.apache.atlas;

import org.apache.commons.configuration.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AtlasClientV2Test {

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    @Test
    public void testIsServerReady() throws AtlasServiceException, AtlasException {
        Configuration configuration = ApplicationProperties.get();
        String[] atlasEndPoint = configuration.getStringArray(ATLAS_ENDPOINT);
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(atlasEndPoint, new String[]{"admin", "admin"});
        Assert.assertTrue(atlasClientV2.isServerReady());
    }
}