package org.apache.hadoop.metadata.service;

import java.io.IOException;

public class TestService implements Service {
	
	public static final String NAME = TestService.class.getName();

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() {
	}

	@Override
	public void close() throws IOException {
	}
}
