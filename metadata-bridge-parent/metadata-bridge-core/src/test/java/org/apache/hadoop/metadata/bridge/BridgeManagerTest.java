package org.apache.hadoop.metadata.bridge;

import javax.inject.Inject;

import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;


@Guice(modules = RepositoryMetadataModule.class)
public class BridgeManagerTest{

	@Inject
	MetadataRepository repo;
	
	@Test
	public void testLoadPropertiesFile() throws Exception {
	BridgeManager bm = new BridgeManager(repo);
	System.out.println(bm.getActiveBridges().size());
	
	Assert.assertEquals(bm.activeBridges.get(0).getClass().getSimpleName(),"HiveLineageBridge");
	}
	
	@Test
	public void testBeanConvertion(){

		//Tests Conversion of Bean to Type
	}
	
	@Test
	public void testIRefConvertion(){

		//Tests Conversion of IRef cast to Bean
	}
	
	
}
