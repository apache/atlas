package org.apache.hadoop.metadata;

import junit.framework.Assert;

import org.apache.hadoop.metadata.services.GraphService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for Guice injector service loading
 */
public class RepositoryServiceLoadingTest extends GuiceEnabledTestBase {
	
	public RepositoryServiceLoadingTest() {
		super(new RepositoryMetadataModule());
	}

	@BeforeClass
	public void setUp() throws Exception {
	}

	@AfterClass
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetGraphService() throws Exception {
		/*
		 * Now that we've got the injector, we can build objects.
		 */
		GraphService gs = injector.getInstance(GraphService.class);
		Assert.assertNotNull(gs);
	}
}
