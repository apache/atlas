package org.apache.hadoop.metadata.service;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for the Service Initializer.
 * 
 * Test functionality to allow loading of different property files.
 */
public class ServiceInitializerTest {

	private final String propertiesFileName = "test.application.properties";
	private ServiceInitializer sinit;

	@BeforeClass
	public void setUp() throws Exception {
		// setup for the test properties file
		System.setProperty(ServiceInitializer.PROPERTIES_SYS_PROP,
				propertiesFileName);
		sinit = new ServiceInitializer();
	}

	@AfterClass
	public void tearDown() throws Exception {
		// test destruction of the Services - no exceptions is assumed a success
		sinit.destroy();
	}

	@Test
	public void testPropsAreSet() throws Exception {
		Assert.assertEquals(
				sinit.getConfiguration().getString(
						"application.services"),
				TestService.NAME);
	}

	@Test
	public void testInitialize() throws Exception {
		// test the initialization of the initializer
		// no exceptions is assumed a success
		sinit.initialize();
	}
}
