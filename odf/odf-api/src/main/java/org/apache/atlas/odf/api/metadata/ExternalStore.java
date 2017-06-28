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
package org.apache.atlas.odf.api.metadata;

import java.util.Properties;

/**
 * A common interface for stores that are external to ODF.
 * Provides connection test methods and basic metadata about the store. 
 *  
 *
 */
public interface ExternalStore {
	static enum ConnectionStatus { OK, AUTHORIZATION_FAILED, UNREACHABLE, UNKOWN_ERROR };
	
	static final String STORE_PROPERTY_DESCRIPTION = "STORE_PROPERTY_DESCRIPTION"; 
	static final String STORE_PROPERTY_TYPE = "STORE_PROPERTY_TYPE"; 
	static final String STORE_PROPERTY_ID = "STORE_PROPERTY_ID"; 
	
	/**
	 * @return the properties of this metadata object store instance.
	 * Must return at least STORE_PROPERTY_DESCRIPTION, STORE_PROPERTY_TYPE, and STORE_PROPERTY_ID.
	 */
	Properties getProperties();
	
	/**
	 * @return the unique repository Id for this metadata store
	 */
	String getRepositoryId();
	
	ConnectionStatus testConnection();
	
}
