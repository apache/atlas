package org.apache.hadoop.metadata;

public abstract class RepositoryModuleBaseTest extends GuiceEnabledTestBase {
	public RepositoryModuleBaseTest() {
		super(new RepositoryMetadataModule());
	}
}
