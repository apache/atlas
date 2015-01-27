/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metadata.bridge.hivelineage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.bridge.hivelineage.hook.HiveLineage;
import org.apache.hadoop.metadata.repository.MetadataRepository;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.gson.Gson;

@Test(enabled = false)
@Guice(modules = RepositoryMetadataModule.class)
public class TestHiveLineageBridge {

	@Inject
	MetadataRepository repo;

	HiveLineageBridge bridge;
	HiveLineage hlb;

	// the id of one.json in the repo (test #1)
	String oneId;

	@BeforeClass
	public void bootstrap() throws IOException {
		// this used in lieu of DI for now
		bridge = new HiveLineageBridge(repo);

		// create a hive lineage bean
		FileInputStream fis = new FileInputStream("one.json");
		List<String> lines = IOUtils.readLines(fis);
		String json = StringUtils.join(lines, "");
		hlb = new Gson().fromJson(json, HiveLineage.class);
	}

	@Test(priority = 1, enabled = false)
	public void testCreate() throws MetadataException {
		// add the lineage bean to the repo
		oneId = bridge.create(hlb);

		// make sure this actually did worked
		Assert.assertNotNull(oneId);
	}

	@Test(priority = 2, enabled = false)
	public void testGet() throws RepositoryException, IOException {
		Object bean = bridge.get(oneId);

		Assert.assertEquals(hlb, bean);
	}

	@Test(priority = 3, enabled = false)
	public void testList() throws RepositoryException {
		List<String> list = IteratorUtils.toList(bridge.list().iterator());
		
		Assert.assertEquals(list.size(), 1);
		Assert.assertEquals(list.get(0), oneId);
	}
}
