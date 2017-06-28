/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.odf.api.spark;

import org.apache.spark.sql.SparkSession;

import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.discoveryservice.SyncDiscoveryServiceBase;

public abstract class SparkDiscoveryServiceBase extends SyncDiscoveryServiceBase implements SparkDiscoveryService  {
	protected SparkSession spark;
	protected MetadataStore mds;

	@Override
	public void setSparkSession(SparkSession spark) {
		this.spark = spark;
	}

	@Override
	public void setMetadataStore(MetadataStore mds) {
		this.mds = mds;
	}
}
