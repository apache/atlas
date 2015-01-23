package org.apache.hadoop.metadata.bridge.hivelineage;

import javax.inject.Inject;

import org.apache.hadoop.metadata.bridge.ABridge;
import org.apache.hadoop.metadata.bridge.hivelineage.hook.HiveLineageBean;
import org.apache.hadoop.metadata.repository.MetadataRepository;

public class HiveLineageBridge extends ABridge {
 
	@Inject
	public HiveLineageBridge(MetadataRepository mr) {
		super(mr);
		this.typeBeanClasses.add(HiveLineageBean.class);
	}  
  
}
