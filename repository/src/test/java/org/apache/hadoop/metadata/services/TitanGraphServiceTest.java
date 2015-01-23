package org.apache.hadoop.metadata.services;

import javax.inject.Inject;

import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.repository.graph.TitanGraphService;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

/**
 * Unit test for TitanGraphService.
 */
@Guice(modules = RepositoryMetadataModule.class)
public class TitanGraphServiceTest {

	@Inject
    TitanGraphService titanGraphService;

    @BeforeClass
    public void setUp() throws Exception {
        titanGraphService.start();
    }

    @AfterClass
    public void tearDown() throws Exception {
        titanGraphService.close();
    }

    @Test
    public void testStart() throws Exception {
        Assert.assertNotNull(titanGraphService.getBlueprintsGraph());
    }

    @Test
    public void testGetBlueprintsGraph() throws Exception {
        Assert.assertNotNull(titanGraphService.getBlueprintsGraph());
    }

    @Test
    public void testGetIndexableGraph() throws Exception {
        Assert.assertNotNull(titanGraphService.getIndexableGraph());
    }

    @Test
    public void testGetTransactionalGraph() throws Exception {
        Assert.assertNotNull(titanGraphService.getTransactionalGraph());
    }

    @Test
    public void testGetTitanGraph() throws Exception {
        Assert.assertNotNull(titanGraphService.getTitanGraph());
    }

    @Test
    public void testGetVertexIndexedKeys() throws Exception {
        Assert.assertNotNull(titanGraphService.getVertexIndexedKeys());
        Assert.assertTrue(titanGraphService.getVertexIndexedKeys().size() > 0);
    }

    @Test
    public void testGetEdgeIndexedKeys() throws Exception {
        Assert.assertNotNull(titanGraphService.getEdgeIndexedKeys());
        Assert.assertTrue(titanGraphService.getEdgeIndexedKeys().size() > 0);
    }
}
