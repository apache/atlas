package org.apache.hadoop.metadata.services;

import org.apache.hadoop.metadata.service.Services;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphBackedMetadataRepositoryServiceTest {

    private static final String ENTITY_NAME = "clicks-table";
    private static final String ENTITY_TYPE = "hive-table";
    private static final String DATABASE_NAME = "ads";
    private static final String TABLE_NAME = "clicks-table";

    private TitanGraphService titanGraphService;
    private GraphBackedMetadataRepositoryService repositoryService;

    @BeforeClass
    public void setUp() throws Exception {
        titanGraphService = new TitanGraphService();
        titanGraphService.start();
        Services.get().register(titanGraphService);

        repositoryService = new GraphBackedMetadataRepositoryService();
        repositoryService.start();
        Services.get().register(repositoryService);
    }

    @AfterClass
    public void tearDown() throws Exception {
        Services.get().getService(GraphBackedMetadataRepositoryService.NAME).close();
        Services.get().getService(TitanGraphService.NAME).close();
        Services.get().reset();
    }

    @Test
    public void testGetName() throws Exception {
        Assert.assertEquals(GraphBackedMetadataRepositoryService.NAME,
                GraphBackedMetadataRepositoryService.class.getSimpleName());
        Assert.assertEquals(repositoryService.getName(), GraphBackedMetadataRepositoryService.NAME);
    }

    @Test
    public void testSubmitEntity() throws Exception {
        String entityStream = getTestEntityJSON();
        String guid = repositoryService.submitEntity(entityStream, ENTITY_TYPE);
        Assert.assertNotNull(guid);
    }

    private String getTestEntityJSON() {
        Map<String, String> props = new HashMap<>();
        props.put("entityName", ENTITY_NAME);
        props.put("entityType", ENTITY_TYPE);
        props.put("database", DATABASE_NAME);
        props.put("table", TABLE_NAME);
        return JSONValue.toJSONString(props);
    }

    @Test (dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        String entity = repositoryService.getEntityDefinition(ENTITY_NAME, ENTITY_TYPE);
        Map<String, String> entityProperties =
                (Map<String, String>) JSONValue.parseWithException(entity);
        Assert.assertNotNull(entityProperties.get("guid"));
        Assert.assertEquals(entityProperties.get("entityName"), ENTITY_NAME);
        Assert.assertEquals(entityProperties.get("entityType"), ENTITY_TYPE);
        Assert.assertEquals(entityProperties.get("database"), DATABASE_NAME);
        Assert.assertEquals(entityProperties.get("table"), TABLE_NAME);
    }

    @Test
    public void testGetEntityDefinitionNonExistent() throws Exception {
        String entity = repositoryService.getEntityDefinition("blah", "blah");
        Assert.assertNull(entity);
    }

    @Test
    public void testGetEntityList() throws Exception {
        List<String> entityList = repositoryService.getEntityList(ENTITY_TYPE);
        Assert.assertNotNull(entityList);
        Assert.assertEquals(entityList.size(), 0); // as this is not implemented yet
    }

    @Test (expectedExceptions = RuntimeException.class)
    public void testStartWithOutGraphServiceRegistration() throws Exception {
        try {
            Services.get().reset();
            GraphBackedMetadataRepositoryService repositoryService = new
                    GraphBackedMetadataRepositoryService();
            repositoryService.start();
            Assert.fail("This should have thrown an exception");
        } finally {
            Services.get().register(titanGraphService);
            Services.get().register(repositoryService);
        }
    }
}
