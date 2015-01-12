package org.apache.hadoop.metadata;

import org.apache.hadoop.metadata.service.Services;
import org.apache.hadoop.metadata.services.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.services.TitanGraphService;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class GraphRepositoryServiceIT {

    private static final String ENTITY_NAME = "clicks-table";
    private static final String ENTITY_TYPE = "hive-table";
    private static final String DATABASE_NAME = "ads";
    private static final String TABLE_NAME = "clicks-table";

    @BeforeClass
    public void setUp() throws Exception {
        TitanGraphService titanGraphService = new TitanGraphService();
        titanGraphService.start();
        Services.get().register(titanGraphService);

        GraphBackedMetadataRepository repositoryService
                = new GraphBackedMetadataRepository();
        repositoryService.start();
        Services.get().register(repositoryService);
    }

    @AfterClass
    public void tearDown() throws Exception {
        Services.get().getService(GraphBackedMetadataRepository.NAME).close();
        Services.get().getService(TitanGraphService.NAME).close();
        Services.get().reset();
    }

/*
    @Test
    public void testRepository() throws Exception {
        GraphBackedMetadataRepositoryService repositoryService =
                Services.get().getService(GraphBackedMetadataRepositoryService.NAME);

        String entityStream = getTestEntityJSON();
        String guid = repositoryService.createEntity(entityStream, ENTITY_TYPE);
        Assert.assertNotNull(guid);

        String entity = repositoryService.getEntityDefinition(ENTITY_NAME, ENTITY_TYPE);
        @SuppressWarnings("unchecked")
        Map<String, String> entityProperties =
                (Map<String, String>) JSONValue.parseWithException(entity);
        Assert.assertEquals(entityProperties.get("guid"), guid);
        Assert.assertEquals(entityProperties.get("entityName"), ENTITY_NAME);
        Assert.assertEquals(entityProperties.get("entityType"), ENTITY_TYPE);
        Assert.assertEquals(entityProperties.get("database"), DATABASE_NAME);
        Assert.assertEquals(entityProperties.get("table"), TABLE_NAME);
    }

    private String getTestEntityJSON() {
        Map<String, String> props = new HashMap<>();
        props.put("entityName", ENTITY_NAME);
        props.put("entityType", ENTITY_TYPE);
        props.put("database", DATABASE_NAME);
        props.put("table", TABLE_NAME);
        return JSONValue.toJSONString(props);
    }
*/
}
