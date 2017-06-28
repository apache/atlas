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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.wink.json4j.JSON;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.connectivity.RESTClientManager;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.json.JSONUtils;

// TODO properly escape all URLs when constructed as string concatenation

/**
 * 
 * A MetadataStore to access metadata via an ODF instance
 *
 */
public class RemoteMetadataStore extends MetadataStoreBase implements MetadataStore {
	private Logger logger = Logger.getLogger(RemoteMetadataStore.class.getName());

	private String odfUrl;
	private String odfUser;

	private Properties mdsProps = null;
	
	// if this is true, null repository Ids are ok for all MetaDataObjectReference objects
	private boolean isDefaultStore = true;

	private RESTClientManager restClient;

	static String ODF_API_INFIX = "/odf/api/v1";

	private void constructThis(String odfUrl, String odfUser, String odfPassword, boolean isDefaultStore) throws URISyntaxException {
		this.odfUrl = odfUrl;
		this.odfUser = odfUser;
		this.restClient = new RESTClientManager(new URI(odfUrl), odfUser, odfPassword);
		this.isDefaultStore = isDefaultStore;
	}

	public RemoteMetadataStore(String odfUrl, String odfUser, String odfPassword, boolean isDefaultStore) throws URISyntaxException, MetadataStoreException {
		constructThis(odfUrl, odfUser, odfPassword, isDefaultStore);
	}

	/**
	 * check if the reference belongs to this repository. Throw exception if not.
	 */
	void checkReference(MetaDataObjectReference reference) {
		if (reference == null) {
			throw new MetadataStoreException("Reference cannot be null");
		}
		if (reference.getRepositoryId() == null) {
			if (!isDefaultStore) {
				throw new MetadataStoreException("Repository ID is not set on the reference.");
			}
		} else {
			if (!reference.getRepositoryId().equals(this.getRepositoryId())) {
				throw new MetadataStoreException(MessageFormat.format("Repository ID ''{0}'' of reference does not match the one of this repository ''{1}''",
						new Object[] { reference.getRepositoryId(), getRepositoryId() }));
			}
		}
	}
	
	/**
	 * check if the ODF metadata API can be reached. Throw exception if not.
	 */
	private void checkConnectionToMetadataAPI() {
		MetadataStore.ConnectionStatus connStatus = testConnection();
		if (connStatus.equals(MetadataStore.ConnectionStatus.UNREACHABLE)) {
			throw new MetadataStoreException("Internal API for metadata store cannot be reached. Make sure that the discovery service has access to the following URL: " + odfUrl);
		} else if (connStatus.equals(MetadataStore.ConnectionStatus.AUTHORIZATION_FAILED)) {
			String messageDetail ="";
			if (this.odfUser.isEmpty()) {
				messageDetail = " Make sure to connect to the discovery service securely through https.";
				//Note that ODF user id and password are only provided if the connection to the service is secure
			}
			throw new MetadataStoreException("Autorization failure when accessing API of internal metadata store." + messageDetail);
		}
	}

	@Override
	public ConnectionInfo getConnectionInfo(MetaDataObject informationAsset) {
		throw new UnsupportedOperationException("This method is not available in the remote implementation of the Metadata store.");
	};

	@Override
	public MetaDataObject retrieve(MetaDataObjectReference reference) {
		checkReference(reference);
		checkConnectionToMetadataAPI();
		try {
			String resource = odfUrl + ODF_API_INFIX + "/metadata/asset/" + URLEncoder.encode(JSONUtils.toJSON(reference), "UTF-8");
			logger.log(Level.FINEST, "Object reference to be retrieved ''{0}''.", reference.toString());
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Get(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code == HttpStatus.SC_NOT_FOUND) {
				return null;
			}
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Retrieval of object ''{0}'' failed: HTTP request status: ''{1}'', {2}",
						new Object[] { JSONUtils.toJSON(reference), statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			} else {
				JSONObject mdo = (JSONObject) JSON.parse(httpResponse.getEntity().getContent());
				mdo.remove("annotations");
				MetaDataObject result = JSONUtils.fromJSON(mdo.write(), MetaDataObject.class);
				if (result.getReference() == null) {
					// An empty JSON documents indicates that the result should be null.
					result = null;
				}
				logger.log(Level.FINEST, "Retrieved metadata object: ''{0}''.", result);
				return result;
			}
		} catch (GeneralSecurityException | IOException | JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
			throw new MetadataStoreException(exc);
		}
	}
	
	@Override
	public Properties getProperties() {
		if (this.mdsProps != null) {
			return this.mdsProps; 
		} else {
			checkConnectionToMetadataAPI();
			try {
				String resource = odfUrl + ODF_API_INFIX + "/metadata";
				Executor executor = this.restClient.getAuthenticatedExecutor();
				HttpResponse httpResponse = executor.execute(Request.Get(resource)).returnResponse();
				StatusLine statusLine = httpResponse.getStatusLine();
				int code = statusLine.getStatusCode();
				InputStream is = httpResponse.getEntity().getContent();
				String response = JSONUtils.getInputStreamAsString(is, "UTF-8");
				is.close();
				if (code != HttpStatus.SC_OK) {
					String msg = MessageFormat.format("Retrieval of metadata store properties at ''{3}'' failed: HTTP request status: ''{0}'', {1}, details: {2}",
							new Object[] { code, statusLine.getReasonPhrase(), response,  resource});
					throw new MetadataStoreException(msg);
				} else {
					this.mdsProps = new Properties();
					JSONObject jo = new JSONObject(response);
					for (Object key : jo.keySet()) {
						this.mdsProps.put((String) key, (String) jo.get(key));
					}
					return this.mdsProps;
				}
			} catch (GeneralSecurityException | IOException | JSONException exc) {
				logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
				throw new MetadataStoreException(exc);
			}			
		}
	}

	@Override
	public List<MetaDataObjectReference> search(String query) {
		checkConnectionToMetadataAPI();
		try {
			logger.log(Level.FINE, "Metadata search term: ''{0}''.", query);
			URIBuilder uri = new URIBuilder(odfUrl + ODF_API_INFIX + "/metadata/search")
					.addParameter("query", query)
					.addParameter("resulttype", "references");
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Get(uri.build())).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				throw new MetadataStoreException("Search request failed: " + statusLine.getStatusCode() + ", " + statusLine.getReasonPhrase());
			}
			InputStream is = httpResponse.getEntity().getContent();
			JSONArray objReferencesJson = new JSONArray(is);
			is.close();
			logger.log(Level.FINEST, "Metadata search response: ''{0}''.", objReferencesJson.write());
			List<MetaDataObjectReference> resultMDORs = new ArrayList<>();
			for (Object ref : objReferencesJson) {
				MetaDataObjectReference objRef = JSONUtils.fromJSON(((JSONObject) ref).write(), MetaDataObjectReference.class);
				resultMDORs.add(objRef);
			}			
			return resultMDORs;
		} catch (GeneralSecurityException | IOException | URISyntaxException | JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting to the metadata store.", exc);
			throw new MetadataStoreException(exc);
		}

	}

	@Override
	public String getRepositoryId() {
		Hashtable<Object, Object> mdsProps = (Hashtable<Object, Object>) this.getProperties();
		if (mdsProps.get(STORE_PROPERTY_ID) != null) {
			return (String) mdsProps.get(STORE_PROPERTY_ID);
		} else {
			throw new MetadataStoreException("Property " + STORE_PROPERTY_ID + " is missing from metadata store properties ''" + mdsProps.toString() + "''.");
		}
	}

	@Override
	public MetadataStore.ConnectionStatus testConnection() {
		return RESTMetadataStoreHelper.testConnectionForStaticURL(restClient, odfUrl);
	}

	@Override
	public void createSampleData() {
		checkConnectionToMetadataAPI();
		try {
			String resource = odfUrl + ODF_API_INFIX + "/metadata/sampledata";
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Get(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Create sample data failed: HTTP request status: ''{1}'', {2}",
						new Object[] { statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			}
		} catch (GeneralSecurityException | IOException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
			throw new MetadataStoreException(exc);
		}
	}

	@Override
	public void resetAllData() {
		checkConnectionToMetadataAPI();
		try {
			String resource = odfUrl + ODF_API_INFIX + "/metadata/resetalldata";
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Post(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Reset all data failed: HTTP request status: ''{1}'', {2}",
						new Object[] { statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			}
		} catch (GeneralSecurityException | IOException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
			throw new MetadataStoreException(exc);
		}
	}

	@Override
	public MetadataQueryBuilder newQueryBuilder() {
		String repoType = getProperties().getProperty(STORE_PROPERTY_TYPE);
		if ("atlas".equals(repoType)) {
			return new AtlasMetadataQueryBuilder();
		} else if ("default".equals(repoType)) {
			return new DefaultMetadataQueryBuilder();
		}
		throw new RuntimeException(MessageFormat.format("No query builder exists for the repository type ''{0}''", repoType));
	}

	@Override
	public AnnotationPropagator getAnnotationPropagator() {
		throw new UnsupportedOperationException("This method is not available in the remote implementation of the Metadata store.");
	}

	protected <T> List<T> getReferences(String attributeName, MetaDataObject metaDataObject, Class<T> type){
		String objectId = metaDataObject.getReference().getId();
		checkConnectionToMetadataAPI();
		try {
			String resource = odfUrl + ODF_API_INFIX + "/metadata/asset/"
				+ URLEncoder.encode(JSONUtils.toJSON(metaDataObject.getReference()), "UTF-8")
				+ "/" + URLEncoder.encode(attributeName.toLowerCase(), "UTF-8");
			logger.log(Level.FINEST, "Retrieving references of type ''{0}'' from metadata object id ''{1}''.", new Object[] { attributeName, objectId });
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Get(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code == HttpStatus.SC_NOT_FOUND) {
				return null;
			}
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Retrieving references of type ''{0}'' of object id ''{1}'' failed: HTTP request status: ''{2}'', {3}",
						new Object[] { attributeName, objectId, statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			} else {
				InputStream is = httpResponse.getEntity().getContent();
				JSONArray objReferencesJson = new JSONArray(is);
				is.close();
				logger.log(Level.FINEST, "Get references response: ''{0}''.", objReferencesJson.write());
				List<T> referencedObjects = new ArrayList<T>();
				for (Object ref : objReferencesJson) {
					T obj = JSONUtils.fromJSON(((JSONObject) ref).write(), type);
					referencedObjects.add(obj);
				}
				return referencedObjects;
			}
		} catch (GeneralSecurityException | IOException | JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
			throw new MetadataStoreException(exc);
		}
	}

	@Override
	public List<MetaDataObject> getReferences(String attributeName, MetaDataObject metaDataObject){
		return getReferences(attributeName, metaDataObject, MetaDataObject.class);
	}

	@Override
	public List<String> getReferenceTypes(){
		checkConnectionToMetadataAPI();
		try {
			String resource = odfUrl + ODF_API_INFIX + "/metadata/referencetypes";
			Executor executor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = executor.execute(Request.Get(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code == HttpStatus.SC_NOT_FOUND) {
				return null;
			}
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Retrieving reference type names failed: HTTP request status: ''{1}'', {2}",
						new Object[] { statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			} else {
				InputStream is = httpResponse.getEntity().getContent();
				JSONArray objReferencesJson = new JSONArray(is);
				is.close();
				logger.log(Level.FINEST, "Get reference types response: ''{0}''.", objReferencesJson.write());
				List<String> referenceTypeNames = new ArrayList<String>();
				for (Object ref : objReferencesJson) {
					String obj = JSONUtils.fromJSON(((JSONObject) ref).write(), String.class);
					referenceTypeNames.add(obj);
				}			
				return referenceTypeNames;
			}
		} catch (GeneralSecurityException | IOException | JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting the metadata store", exc);
			throw new MetadataStoreException(exc);
		}
	}

	@Override
	public MetaDataObject getParent(MetaDataObject metaDataObject){
		List<MetaDataObject> parentList = getReferences(InternalMetaDataUtils.ODF_PARENT_REFERENCE, metaDataObject, MetaDataObject.class);
		if (parentList.size() == 1) {
			return parentList.get(0);
		} else if (parentList.size() == 0) {
			return null;
		}
		String errorMessage = MessageFormat.format("Inconsistent object reference: Metadata object with id ''{0}'' refers to more that one parent object.", metaDataObject.getReference().getId());
		throw new MetadataStoreException(errorMessage);
	}

	@Override
	public List<MetaDataObject> getChildren(MetaDataObject metaDataObject){
		return getReferences(InternalMetaDataUtils.ODF_CHILDREN_REFERENCE, metaDataObject, MetaDataObject.class);
	}

}
