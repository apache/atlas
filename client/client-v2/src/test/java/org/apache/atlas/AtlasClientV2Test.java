package org.apache.atlas;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AtlasClientV2Test {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasClientV2Test.class);

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    AtlasClientV2 atlasClientV2;

    @BeforeClass
    public void before() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        String[] atlasEndPoint = configuration.getStringArray(ATLAS_ENDPOINT);
        atlasClientV2 = new AtlasClientV2(atlasEndPoint, new String[]{"admin", "admin"});
    }

    @Test
    public void testIsServerReady() throws AtlasServiceException {
        Assert.assertTrue(atlasClientV2.isServerReady());
    }

    @Test
    public void testGetEntityDefByName() throws AtlasServiceException {
        AtlasEntityDef info = atlasClientV2.getEntityDefByName("hive_db");
        AtlasEntityDef def = atlasClientV2.getEntityDefByGuid(info.getGuid());
    }

    @Test
    public void testGetEntityByGuid() throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo info = atlasClientV2.getEntityByGuid("53b4f45d-5c84-4984-b818-05607b9bf6d7");
    }

    @Test
    public void testUpdateEntity() throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo info = atlasClientV2.getEntityByGuid("53b4f45d-5c84-4984-b818-05607b9bf6d7");
        AtlasEntity entity = info.getEntity();
        entity.setAttribute("extInfo", "This is a test info for adding attribute info");
        LOG.debug("attribute extInfo = " + entity.getAttribute("extInfo"));
        info.setEntity(entity);
        atlasClientV2.updateEntity(info);
    }

    @Test
    public void testRemoveEntity() throws AtlasServiceException {
        atlasClientV2.deleteEntityByGuid("6a25bdb4-78c8-4277-8bbf-8ce1e9a36d66");
    }

    @Test
    public void testtestDeleteRelationShip() throws AtlasServiceException {
        atlasClientV2.deleteRelationshipByGuid("guid");
    }

    @Test
    public void testSearch() throws AtlasServiceException {
        AtlasSearchResult result = atlasClientV2.basicSearch("hive_db", null, null, true, 10, 0);
    }
}