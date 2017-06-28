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
package org.apache.atlas.odf.core.metadata.atlas;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.connectivity.RESTClientManager;
import org.apache.atlas.odf.api.metadata.*;
import org.apache.atlas.odf.core.Encryption;
import org.apache.atlas.odf.core.Environment;
import org.apache.atlas.odf.core.ODFInternalFactory;
import org.apache.atlas.odf.core.metadata.SampleDataHelper;
import org.apache.atlas.odf.core.metadata.WritableMetadataStore;
import org.apache.atlas.odf.core.metadata.WritableMetadataStoreBase;
import org.apache.atlas.odf.core.metadata.WritableMetadataStoreUtils;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.metadata.AnnotationPropagator;
import org.apache.atlas.odf.api.metadata.AtlasMetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.InternalMetaDataUtils;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.RESTMetadataStoreHelper;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.Column;
import org.apache.atlas.odf.api.metadata.models.Connection;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.DataFile;
import org.apache.atlas.odf.api.metadata.models.DataFileFolder;
import org.apache.atlas.odf.api.metadata.models.DataStore;
import org.apache.atlas.odf.api.metadata.models.Database;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.Schema;
import org.apache.atlas.odf.api.metadata.models.Table;
import com.google.common.collect.Lists;
import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.annotation.AnnotationStoreUtils;

// TODO properly escape all URLs when constructed as string concatenation

/**
 *
 * A MetadataStore implementation for accessing metadata stored in an atlas instance
 *
 */
public class AtlasMetadataStore extends WritableMetadataStoreBase implements MetadataStore, WritableMetadataStore {
	private Logger logger = Logger.getLogger(AtlasMetadataStore.class.getName());

	private static HashMap<String, StoredMetaDataObject> objectStore; // Not actually used but required to meet needs of InternalMetadataStoreBase
	protected LinkedHashMap<String, StoredMetaDataObject> stagedObjects = new LinkedHashMap<String, StoredMetaDataObject>();
	protected static Object accessLock = new Object();

	private String url;

	private String storeId;

	private RESTClientManager restClient;

	private AtlasModelBridge modelBridge;

	static String ATLAS_API_INFIX = "/api/atlas";

	private void constructThis(String url, String user, String password) throws URISyntaxException {
		this.url = url;
		this.storeId = "atlas:" + url;
		this.restClient = new RESTClientManager(new URI(url), user, password);
		this.modelBridge = new AtlasModelBridge(this);
	}

	public AtlasMetadataStore() throws URISyntaxException {
		Environment env = new ODFInternalFactory().create(Environment.class);
		String atlasURL = env.getProperty("atlas.url");
		String atlasUser = env.getProperty("atlas.user");
		String atlasPassword = env.getProperty("atlas.password");
		if ((atlasURL == null) || atlasURL.isEmpty() || (atlasUser == null) || atlasUser.isEmpty() || (atlasPassword == null) || atlasPassword.isEmpty())  {
			throw new RuntimeException("The system variables \"atlas.url\", \"atlas.user\", and \"atlas.password\" must be set.");
		}
		constructThis(atlasURL, atlasUser, Encryption.decryptText(atlasPassword));
	}

	protected Object getAccessLock() {
		return accessLock;
	}

	// Not actually used but required to meet needs of InternalMetadataStoreBase
	protected HashMap<String, StoredMetaDataObject> getObjects() {
		return objectStore;
	}

	protected LinkedHashMap<String, StoredMetaDataObject> getStagedObjects() {
		return stagedObjects;
	}

	public static final int TIMEOUT = 2000;

	static Object ensureTypesLock = new Object();

	public void ensureODFTypesExist() {
		synchronized (ensureTypesLock) {
			try {
				String typesTestURI = this.url + ATLAS_API_INFIX + "/types/MetaDataObject";
				Executor executor = this.restClient.getAuthenticatedExecutor();
				HttpResponse httpResponse = executor.execute(Request.Get(typesTestURI)).returnResponse();

				StatusLine statusLine = httpResponse.getStatusLine();
				int statusCode = statusLine.getStatusCode();
				if (statusCode == HttpStatus.SC_OK) {
					return;
				}
				if (statusCode != HttpStatus.SC_NOT_FOUND) {
					throw new MetadataStoreException("An error occurred when checking for Atlas types. Code: " + statusCode + ", reason: " + statusLine.getReasonPhrase());
				}
				// now create types
				InputStream is = this.getClass().getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/metadata/internal/atlas/atlas-odf-model.json");
				Request createTypesRequest = Request.Post(this.url + ATLAS_API_INFIX + "/types");
				createTypesRequest.bodyStream(is, ContentType.APPLICATION_JSON);
				httpResponse = executor.execute(createTypesRequest).returnResponse();
				statusLine = httpResponse.getStatusLine();
				statusCode = statusLine.getStatusCode();
				if (statusCode != HttpStatus.SC_CREATED) {
					throw new MetadataStoreException("An error occurred while creating ODF types in Atlas. Code: " + statusCode + ", reason: " + statusLine.getReasonPhrase());
				}
			} catch (GeneralSecurityException | IOException e) {
				logger.log(Level.FINE, "An unexpected exception ocurred while connecting to Atlas", e);
				throw new MetadataStoreException(e);
			}
		}

	}

	private void checkConnectivity() {
		ensureODFTypesExist();
	}

	/* Filter out all types that exist.
	 * This is necessary because trying to create a type multiple times
	 * will lead to a 503 error after Atlas is restarted with an error saying
	 * "Type extends super type multiple times"
	 *
	 * Returns true if some filteringTookPlace
	 *
	 * Note: Trying to remove the super types from the request doesn't work either.
	 */
	boolean filterExistingTypes(JSONObject atlasTypeDefinitions, String typeProperty) throws GeneralSecurityException, IOException {
		boolean filterWasApplied = false;
		JSONArray types = (JSONArray) atlasTypeDefinitions.opt(typeProperty);
		JSONArray newTypes = new JSONArray();
		for (Object typeObj : types) {
			JSONObject type = (JSONObject) typeObj;

			Executor executor = this.restClient.getAuthenticatedExecutor();
			String typeName = (String) type.opt("typeName");
			if (typeName != null) {
				Request checkTypeRequest = Request.Get(this.url + ATLAS_API_INFIX + "/types/" + typeName);
				HttpResponse httpResponse = executor.execute(checkTypeRequest).returnResponse();
				StatusLine statusLine = httpResponse.getStatusLine();
				int statusCode = statusLine.getStatusCode();
				if (statusCode != HttpStatus.SC_NOT_FOUND) {
					// type already exists, don't create it
					filterWasApplied = true;
					logger.log(Level.FINE, "Atlas type ''{0}'' already exists, don't create it again", typeName);
				} else {
					newTypes.add(type);
				}
			}
		}

		try {
			atlasTypeDefinitions.put(typeProperty, newTypes);
		} catch (JSONException e) {
			throw new RuntimeException(e); // should never happen as only proper JSONObjects are used
		}
		return filterWasApplied;
	}

	boolean isInvalidTypeRequest(JSONObject atlasTypeDefinition) {
		return ((JSONArray) atlasTypeDefinition.opt("structTypes")).isEmpty() //
				&& ((JSONArray) atlasTypeDefinition.opt("enumTypes")).isEmpty() //
				&& ((JSONArray) atlasTypeDefinition.opt("classTypes")).isEmpty() //
				&& ((JSONArray) atlasTypeDefinition.opt("traitTypes")).isEmpty();
	}

	void checkUpdateForKnownType(JSONObject atlasTypeDefinition) {
		JSONArray types = (JSONArray) atlasTypeDefinition.opt("classTypes");
		for (Object o : types) {
			JSONObject type = (JSONObject) o;
			String typeName = (String) type.opt("typeName");
			if ("ODFAnnotation".equals(typeName)) {
				String msg = MessageFormat.format("Update of type ''{0}'' is not allowed", typeName);
				throw new MetadataStoreException(msg);
			}
		}
	}

	public boolean createType(JSONObject atlasTypeDefinition) {
		try {
			logger.log(Level.FINE, "Creating types with definition: {0}", atlasTypeDefinition.write());
			checkConnectivity();
			boolean filterWasApplied = this.filterExistingTypes(atlasTypeDefinition, "classTypes");
			filterWasApplied |= this.filterExistingTypes(atlasTypeDefinition, "structTypes");
			String typesDef = atlasTypeDefinition.write();
			if (filterWasApplied) {
				logger.log(Level.FINE, "Modified type definitions after filtering exiting types: {0}", typesDef);
			}
			if (isInvalidTypeRequest(atlasTypeDefinition)) {
				logger.log(Level.FINE, "No types left to be created after filtering, skipping");
				return false;
			}
			Executor executor = this.restClient.getAuthenticatedExecutor();
			Request createTypesRequest = Request.Put(this.url + ATLAS_API_INFIX + "/types");
			createTypesRequest.bodyStream(new ByteArrayInputStream(typesDef.getBytes("UTF-8")), ContentType.APPLICATION_JSON);
			HttpResponse httpResponse = executor.execute(createTypesRequest).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int statusCode = statusLine.getStatusCode();
			if (statusCode != HttpStatus.SC_OK) {
				throw new MetadataStoreException("An error occurred while creating ODF types in Atlas. Code: " + statusCode + ", reason: " + statusLine.getReasonPhrase());
			}
			logger.log(Level.FINE, "Types created. Original request: {0}", typesDef);
		} catch (GeneralSecurityException | IOException | JSONException e) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting to Atlas", e);
			throw new MetadataStoreException(e);
		}
		return true;

	}

	public JSONObject getAtlasTypeDefinition(String typeName) {
		try {
			checkConnectivity();
			HttpResponse httpResponse = this.restClient.getAuthenticatedExecutor().execute(Request.Get(this.url + ATLAS_API_INFIX + "/types/" + typeName)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int statusCode = statusLine.getStatusCode();
			if (statusCode == HttpStatus.SC_OK) {
				InputStream is = httpResponse.getEntity().getContent();
				JSONObject typeResp = new JSONObject(is);
				is.close();
				return typeResp;
			}
			return null;
		} catch (GeneralSecurityException | IOException | JSONException e) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting to Atlas", e);
			throw new MetadataStoreException(e);
		}
	}

    @Override
    public ConnectionInfo getConnectionInfo(MetaDataObject informationAsset) {
   		return WritableMetadataStoreUtils.getConnectionInfo(this, informationAsset);
    };

	@Override
	public MetaDataObject retrieve(MetaDataObjectReference reference) {
		checkConnectivity();
		synchronized (updateLock) {
			return this.retrieve(reference, 0);
		}
	}

	MetaDataObject retrieve(MetaDataObjectReference reference, int level) {
		JSONObject objectJson = retrieveAtlasEntityJson(reference);
		if (objectJson == null) {
			return null;
		}
		try {
			MetaDataObject mdo = this.modelBridge.createMetaDataObjectFromAtlasEntity(objectJson, level);
			return mdo;
		} catch (JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting to Atlas", exc);
			throw new MetadataStoreException(exc);
		}
	}

	JSONObject retrieveAtlasEntityJson(MetaDataObjectReference reference) {
		modelBridge.checkReference(reference);
		String id = reference.getId();
		try {
			String resource = url + ATLAS_API_INFIX + "/entities/" + id;
			HttpResponse httpResponse = this.restClient.getAuthenticatedExecutor().execute(Request.Get(resource)).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code == HttpStatus.SC_NOT_FOUND) {
				return null;
			}
			if (code != HttpStatus.SC_OK) {
				String msg = MessageFormat.format("Retrieval of object ''{0}'' failed: HTTP request status: ''{1}'', {2}",
						new Object[] { id, statusLine.getStatusCode(), statusLine.getReasonPhrase() });
				throw new MetadataStoreException(msg);
			} else {
				InputStream is = httpResponse.getEntity().getContent();
				JSONObject jo = new JSONObject(is);
				is.close();
				return jo;
			}
		} catch (GeneralSecurityException | IOException | JSONException exc) {
			logger.log(Level.WARNING, "An unexpected exception ocurred while connecting to Atlas", exc);
			throw new MetadataStoreException(exc);
		}
	}

	// TODO only helps in single server case
	// this is just a temporary workaround around the fact that Atlas does not update bidirectional
	// references.
	// TODO this currently prevents deadlocks from happening, needs to be reworked for distributed case
	static Object updateLock = new Object();

	private MetaDataObjectReference storeJSONObject(JSONObject jsonObject) {
		logger.log(Level.FINEST, "Storing converted Atlas object: {0}.", JSONUtils.jsonObject4Log(jsonObject));
		synchronized(updateLock) {
			try {
				Executor restExecutor = this.restClient.getAuthenticatedExecutor();
				HttpResponse atlasResponse = restExecutor.execute( //
						Request.Post(this.url + ATLAS_API_INFIX + "/entities") //
								.bodyString(jsonObject.write(), ContentType.APPLICATION_JSON) //
				).returnResponse();
				InputStream is = atlasResponse.getEntity().getContent();
				JSONObject atlasResult = new JSONObject(is);
				is.close();
				StatusLine line = atlasResponse.getStatusLine();
				int statusCode = line.getStatusCode();
				if (statusCode != HttpStatus.SC_CREATED) {
					logger.log(Level.SEVERE, "Atlas REST call failed, return code: {0}, reason: {1}", new Object[] { statusCode, line.getReasonPhrase() });
					logger.log(Level.WARNING, "Atlas could not create object for request: {0}", jsonObject.write());
					logger.log(Level.WARNING, "Atlas result for creating object: {0}", atlasResult.write());
					throw new MetadataStoreException(
							MessageFormat.format("Atlas REST call failed, return code: {0}, reason: {1}, details: {2}", new Object[] { statusCode, line.getReasonPhrase(), atlasResult.write() }));
				}
				logger.log(Level.FINEST, "Atlas response for storing object: {0}", JSONUtils.jsonObject4Log(atlasResult));
				JSONArray ids = (JSONArray) ((JSONObject) atlasResult.get("entities")).get("created");
				if (ids.size() != 1) {
					String msg = "More than one (or no) Atlas entities have been created. Need a unique entity to be referenced by other objects.";
					throw new MetadataStoreException(msg);
				}
				String newAnnotationId = (String) ids.get(0);
				MetaDataObjectReference result = new MetaDataObjectReference();
				result.setRepositoryId(getRepositoryId());
				result.setId(newAnnotationId);
				result.setUrl(getURL(newAnnotationId));
				return result;
			} catch (JSONException e) {
				throw new MetadataStoreException(MessageFormat.format("Error converting JSON object ''{0}'' to string", JSONUtils.jsonObject4Log(jsonObject)), e);
			} catch(IOException | GeneralSecurityException e2) {
				throw new MetadataStoreException(MessageFormat.format("Error storing object ''{0}'' in Atlas", JSONUtils.jsonObject4Log(jsonObject)), e2);
			}
		}
	}

	private void updateJSONObject(JSONObject jsonObject, String id) {
		logger.log(Level.FINEST, "Updating converted Atlas object: {0}.",JSONUtils.jsonObject4Log(jsonObject));
		synchronized(updateLock) {
			try {
				Executor restExecutor = this.restClient.getAuthenticatedExecutor();
				HttpResponse atlasResponse = restExecutor.execute( //
						Request.Post(this.url + ATLAS_API_INFIX + "/entities/" + id) //
								.bodyString(jsonObject.write(), ContentType.APPLICATION_JSON) //
				).returnResponse();
				InputStream is = atlasResponse.getEntity().getContent();
				JSONObject atlasResult = new JSONObject(is);
				is.close();
				StatusLine line = atlasResponse.getStatusLine();
				int statusCode = line.getStatusCode();
				if (statusCode != HttpStatus.SC_OK) {
					logger.log(Level.WARNING, "Atlas could not update object with request: {0}", jsonObject.write());
					throw new MetadataStoreException(
							MessageFormat.format("Atlas REST call failed, return code: {0}, reason: {1}, details: {2}", new Object[] { statusCode, line.getReasonPhrase(), atlasResult.write() }));
				}
				logger.log(Level.FINEST, "Atlas response for updating object: {0}", JSONUtils.jsonObject4Log(atlasResult));
			} catch (JSONException e) {
				throw new MetadataStoreException(MessageFormat.format("Error converting JSON object ''{0}'' to string", JSONUtils.jsonObject4Log(jsonObject)), e);
			} catch(IOException | GeneralSecurityException e2) {
				throw new MetadataStoreException(MessageFormat.format("Error storing object ''{0}'' in Atlas", JSONUtils.jsonObject4Log(jsonObject)), e2);
			}
		}
	}

	private MetaDataObjectReference store(Annotation annot) {
		checkConnectivity();
		synchronized (updateLock) {
			try {
				JSONObject annotationJSON = this.modelBridge.createAtlasEntityJSON(new StoredMetaDataObject(annot), new HashMap<String, String>(), new HashMap<String, MetaDataObjectReference>(), null);
				MetaDataObjectReference newObjectRef = storeJSONObject(annotationJSON);

				////////////////////////////////////////
				// set inverse explicitly, remove this until Atlas does it automatically

				// first get full annotated object
				String annotatedObjectId = AnnotationStoreUtils.getAnnotatedObject(annot).getId();
				Executor restExecutor = this.restClient.getAuthenticatedExecutor();
				HttpResponse atlasResponse = restExecutor.execute(Request.Get(this.url + ATLAS_API_INFIX + "/entities/" + annotatedObjectId)).returnResponse();
				StatusLine line = atlasResponse.getStatusLine();
				int statusCode = line.getStatusCode();
				if (statusCode != HttpStatus.SC_OK) {
					logger.log(Level.SEVERE, "Atlas REST call failed, return code: {0}, reason: {1}", new Object[] { statusCode, line.getReasonPhrase() });
					logger.log(Level.WARNING, "Atlas could not retrieve annotated object: {0}", annotatedObjectId);
					return null;
				}

				InputStream is = atlasResponse.getEntity().getContent();
				JSONObject annotatedObject = new JSONObject(is).getJSONObject("definition");
				is.close();
				JSONObject annotatedObjectValues = ((JSONObject) annotatedObject.get("values"));
				JSONArray annotations = (JSONArray) annotatedObjectValues.opt("annotations");

				// add new "annotations" object to list
				if (annotations == null) {
					annotations = new JSONArray();
					annotatedObjectValues.put("annotations", annotations);
				}
				JSONObject annotationRef = modelBridge.createAtlasObjectReference(newObjectRef.getId(), "ODFAnnotation");
				annotations.add(annotationRef);

				// now update
				atlasResponse = restExecutor.execute(Request.Post(this.url + ATLAS_API_INFIX + "/entities/" + annotatedObjectId).bodyString(annotatedObject.write(), ContentType.APPLICATION_JSON))
						.returnResponse();
				line = atlasResponse.getStatusLine();
				statusCode = line.getStatusCode();
				if (statusCode != HttpStatus.SC_OK) {
					logger.log(Level.SEVERE, "Atlas REST call failed, return code: {0}, reason: {1}", new Object[] { statusCode, line.getReasonPhrase() });
					logger.log(Level.WARNING, "Atlas could not update annotated object: {0}", annotatedObjectId);
					return null;
				}

				return newObjectRef;
			} catch (MetadataStoreException e) {
				throw e;
			} catch (Exception e) {
				throw new MetadataStoreException(e);
			}
		}
	}

	private boolean deleteAcyclic(MetaDataObjectReference reference,
			                      HashSet<MetaDataObjectReference> referencesProcessed) {
		try {
			List<Annotation> annotations = this.getAnnotations(reference, null);
			if (annotations != null) {
				for (Annotation annotation : annotations) {
					if (referencesProcessed.contains(annotation.getReference())) {
						throw new MetadataStoreException("Circular annotation definition found: " + annotation.getReference().getRepositoryId());
					}
					referencesProcessed.add(annotation.getReference());
					deleteAcyclic(annotation.getReference(), referencesProcessed);
				}
			}
			URIBuilder uri = new URIBuilder(url + ATLAS_API_INFIX + "/entities").addParameter("guid", reference.getId());
			Executor restExecutor = this.restClient.getAuthenticatedExecutor();
			HttpResponse httpResponse = restExecutor.execute(Request.Delete(uri.build())).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				throw new MetadataStoreException("Search request failed: " + statusLine.getStatusCode() + ", " + statusLine.getReasonPhrase());
			}
			InputStream is = httpResponse.getEntity().getContent();
			JSONObject jo = new JSONObject(is);
			is.close();
			if (jo.containsKey("entities")) {
				JSONObject entities = jo.getJSONObject("entities");
				if (entities.containsKey("deleted")) {
					JSONArray deleted = entities.getJSONArray("deleted");
					return (deleted.size() == 1 && deleted.getString(0).equals(reference.getId()));
				}
			}
			return false;
		} catch(Exception exc) {
			throw new MetadataStoreException(exc);
		}
	}

	// TODO: Implement 'delete cascade'. Currently this only works for annotations but not for other types of object relationships

	private boolean delete(MetaDataObjectReference reference) {
		checkConnectivity();
		return deleteAcyclic(reference, new HashSet<MetaDataObjectReference>());
	}

	@Override
	public Properties getProperties() {
		Properties props = new Properties();
		props.put(STORE_PROPERTY_DESCRIPTION, MessageFormat.format("An Atlas metadata repository at ''{0}''", url));
		props.put(STORE_PROPERTY_TYPE, "atlas");
		props.put(STORE_PROPERTY_ID, this.storeId);
		props.put("atlas.url", url);
		return props;
	}

	/**
	 * Returns a "human-readable" URL for this object, typically pointing to the Atlas UI.
	 */
	public String getURL(String guid) {
		return url + "/#!/detailPage/" + guid;
	}

	public String getAtlasUrl() {
		return this.url;
	}

	@Override
	/**
	 * Search query is passed into generic API (Gremlin, DSL, or fulltext) are selected under the covers.
	 */
	public List<MetaDataObjectReference> search(String query) {
		checkConnectivity();
		try {
			URIBuilder uri = null;
			HttpResponse httpResponse = null;
			Executor restExecutor = this.restClient.getAuthenticatedExecutor();
			if (query.startsWith("g.V")) {
				uri = new URIBuilder(url + ATLAS_API_INFIX + "/discovery/search/gremlin").addParameter("query", query);
				httpResponse = restExecutor.execute(Request.Get(uri.build())).returnResponse();
			} else {
				uri = new URIBuilder(url + ATLAS_API_INFIX + "/discovery/search").addParameter("query", query);
				httpResponse = restExecutor.execute(Request.Get(uri.build())).returnResponse();
			}
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				throw new MetadataStoreException("Search request failed: " + statusLine.getStatusCode() + ", " + statusLine.getReasonPhrase());
			}
			InputStream is = httpResponse.getEntity().getContent();
			JSONObject jo = new JSONObject(is);
			is.close();
			String querytype = (String) jo.get("queryType");

			String repoId = getRepositoryId();
			List<MetaDataObjectReference> resultMDORs = new ArrayList<>();
			JSONArray resultList = (JSONArray) jo.get("results");
			for (Object o : resultList) {
				JSONObject result = (JSONObject) o;
				String guid = null;
				// get GUID differently depending on the query type
				if ("gremlin".equals(querytype)) {
					guid = (String) result.get("__guid");
				} else if ("dsl".equals(querytype)) {
					guid = (String) ((JSONObject) result.get("$id$")).get("id");
				} else {
					guid = (String) result.get("guid");
				}
				MetaDataObjectReference ref = new MetaDataObjectReference();
				ref.setId(guid);
				ref.setRepositoryId(repoId);
				ref.setUrl(getURL(guid));
				resultMDORs.add(ref);
			}
			return resultMDORs;
		} catch (Exception exc) {
			throw new MetadataStoreException(exc);
		}

	}

	@Override
	public String getRepositoryId() {
		return this.storeId;
	}

	@Override
	public MetadataStore.ConnectionStatus testConnection() {
		return RESTMetadataStoreHelper.testConnectionForStaticURL(restClient, url);
	}

	// Make sure Atlas objects are deleted in a particular order according to foreign key relationships to prevent objects from becoming orphans
	private static final String[] deletionSequence = new String[]{"Annotation", "BusinessTerm", "DataStore", "DataFileFolder", "DataSet" };

	@Override
	public void resetAllData() {
		logger.info("Resetting all data on the metadata repository");
		for (String typeToDelete:deletionSequence) {
			List<MetaDataObjectReference> refs = this.search("from " + typeToDelete);
			int i = 0;
			for (MetaDataObjectReference ref : refs) {
				try {
					this.delete(ref);
					i++;
				} catch(Exception exc) {
					logger.log(Level.WARNING, MessageFormat.format("Object ''{0}'' could not be deleted", ref.getId()), exc);
				}
			}
			logger.info(i + " objects of type " + typeToDelete + " deleted.");
		}
	}

	public Annotation retrieveAnnotation(MetaDataObjectReference annotationRef) {
		MetaDataObject mdo = this.retrieve(annotationRef);
		if (mdo instanceof Annotation) {
			return (Annotation) mdo;
		}
		throw new MetadataStoreException(MessageFormat.format("Object with id ''{0}'' is not an annotation", annotationRef.getId()));
	}



	@SuppressWarnings("unchecked")
	private List<JSONObject> runAnnotationQuery(String query) {
		try {
			List<JSONObject> results = new ArrayList<>();
			Executor restExecutor = this.restClient.getAuthenticatedExecutor();
			URIBuilder uri = new URIBuilder(url + ATLAS_API_INFIX + "/discovery/search/dsl").addParameter("query",
					query);
			HttpResponse httpResponse = restExecutor.execute(Request.Get(uri.build())).returnResponse();
			StatusLine statusLine = httpResponse.getStatusLine();
			int code = statusLine.getStatusCode();
			if (code != HttpStatus.SC_OK) {
				throw new MetadataStoreException(
						"Search request failed: " + statusLine.getStatusCode() + ", " + statusLine.getReasonPhrase());
			}
			InputStream is = httpResponse.getEntity().getContent();
			JSONObject jo = new JSONObject(is);
			is.close();
			results.addAll(jo.getJSONArray("results"));
			return results;
		} catch (Exception exc) {
			throw new MetadataStoreException(exc);
		}
	}

	private String combineToWhereClause(List<String> clauses) {
		StringBuilder whereClause = null;
		for (String clause : clauses) {
			if (clause != null) {
				if (whereClause == null) {
					whereClause = new StringBuilder("where ");
					whereClause.append(clause);
				} else {
					whereClause.append(" and ").append(clause);
				}
			}
		}
		if (whereClause == null) {
			whereClause = new StringBuilder("");
		}
		return whereClause.toString();
	}

	private List<Annotation> getAnnotations(MetaDataObjectReference object, String analysisRequestId) {
		checkConnectivity();

		String profilingAnnotationObjectClause = null;
		String classificationAnnotationObjectClause = null;
		String analysisRequestClause = null;
		if (object != null) {
		 	profilingAnnotationObjectClause = "t.profiledObject.__guid = '" + object.getId() + "'";
		 	classificationAnnotationObjectClause = "t.classifiedObject.__guid = '" + object.getId() + "'";
		}
		if (analysisRequestId != null) {
			analysisRequestClause = "t.analysisRun = '" + analysisRequestId + "'";
		}

		List<JSONObject> queryResults = new ArrayList<>();
		queryResults.addAll(runAnnotationQuery(
				"from ProfilingAnnotation as t " + combineToWhereClause(Arrays.asList(new String[]{profilingAnnotationObjectClause, analysisRequestClause})) ));
		queryResults.addAll(runAnnotationQuery(
				"from ClassificationAnnotation as t " + combineToWhereClause(Arrays.asList(new String[]{classificationAnnotationObjectClause, analysisRequestClause})) ));
		// TODO relationship annotation

		try {
			List<Annotation> results = new ArrayList<>();
			for (JSONObject jo : queryResults) {
				results.add((Annotation) this.modelBridge.createMetaDataObjectFromAtlasSearchResult(jo, 0));
			}
			return results;
		} catch (Exception exc) {
			exc.printStackTrace();
			throw new MetadataStoreException(exc);
		}
	}

	@Override
	public void createSampleData() {
		logger.log(Level.INFO, "Creating sample data in metadata store.");
		SampleDataHelper.copySampleFiles();
		WritableMetadataStoreUtils.createSampleDataObjects(this);
	}

	@Override
	public MetadataQueryBuilder newQueryBuilder() {
		return new AtlasMetadataQueryBuilder();
	}

	public static void main(String[] args) {
		try {
			System.out.println("Creating Atlas sample data.");
			AtlasMetadataStore mds = new AtlasMetadataStore();
			mds.createSampleData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public AnnotationPropagator getAnnotationPropagator() {
		return new AnnotationPropagator() {

			@Override
			public void propagateAnnotations(AnnotationStore as, String requestId) {
				if (as instanceof AtlasMetadataStore) {
					// do nothing, annotations already persisted
					return;
				}
				// if this is another annotation store, simply store the annotations as-is
				List<Annotation> annotations = as.getAnnotations(null, requestId);
				for (Annotation annot : annotations) {
					store(annot);
				}
			}
		};
	}

	@Override
	public void commit() {
		checkConnectivity();
		HashMap<String, StoredMetaDataObject> objectHashMap = new HashMap<String, StoredMetaDataObject>();
		HashMap<String, String> typeMap = new HashMap<String, String>();
		for (StoredMetaDataObject object : stagedObjects.values()) {
			MetaDataObjectReference objRef = object.getMetaDataObject().getReference();
			modelBridge.checkReference(objRef);
			objectHashMap.put(objRef.getId(), object);
			typeMap.put(objRef.getId(), object.getMetaDataObject().getClass().getSimpleName());
		}

		// Create a list of all objects, starting with "root objects" that do not have dependencies on the subsequent objects
		List<StoredMetaDataObject> objectsToCreate = new ArrayList<StoredMetaDataObject>();
		int numberOfObjectsToCreate;
		do {
			List<StoredMetaDataObject> rootObjectList = modelBridge.getRootObjects(objectHashMap);
			numberOfObjectsToCreate = objectsToCreate.size();
			objectsToCreate.addAll(rootObjectList);
			for (StoredMetaDataObject rootObject : rootObjectList) {
				objectHashMap.remove(rootObject.getMetaDataObject().getReference().getId());
			}
		} while((objectHashMap.size() > 0) && (objectsToCreate.size() > numberOfObjectsToCreate));

		// Process object list in reverse order so that dependent objects are created first
		HashMap<String, MetaDataObjectReference> referenceMap = new HashMap<String, MetaDataObjectReference>();
		for (StoredMetaDataObject obj : Lists.reverse(objectsToCreate)) {
			if (retrieve(obj.getMetaDataObject().getReference()) != null) {
				// Update existing object
				JSONObject originalAtlasJson = retrieveAtlasEntityJson(obj.getMetaDataObject().getReference());
				JSONObject newObjectJSON = modelBridge.createAtlasEntityJSON(obj, typeMap, referenceMap, originalAtlasJson);
				logger.log(Level.INFO, "Updating object of type ''{0}'' in metadata store: ''{1}''", new Object[] { obj.getClass().getName(), newObjectJSON });
				updateJSONObject(newObjectJSON, obj.getMetaDataObject().getReference().getId());
			} else {
				// Create new object
				JSONObject newObjectJSON = modelBridge.createAtlasEntityJSON(obj, typeMap, referenceMap, null);
				logger.log(Level.INFO, "Storing new object of type ''{0}'' in metadata store: ''{1}''", new Object[] { obj.getClass().getName(), newObjectJSON });
				referenceMap.put(obj.getMetaDataObject().getReference().getId(), storeJSONObject(newObjectJSON)); // Store new object id in reference map
			}
		}
	}

	@Override
	public MetaDataObject getParent(MetaDataObject metaDataObject) {
		String queryString = "";
		Class<? extends MetaDataObject> type = MetaDataObject.class;
		String objectId = metaDataObject.getReference().getId();
		if (metaDataObject instanceof Column) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__RelationalDataSet.columns\").toList()";
			type = RelationalDataSet.class;
		} else if (metaDataObject instanceof Connection) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__DataStore.connections\").toList()";
			type = DataStore.class;
		} else if (metaDataObject instanceof DataFileFolder) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__DataFileFolder.dataFileFolders\").toList()";
			type = DataFileFolder.class;
		} else if (metaDataObject instanceof DataFile) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__DataFileFolder.dataFiles\").toList()";
			type = DataFileFolder.class;
		} else if (metaDataObject instanceof Schema) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__Database.schemas\").toList()";
			type = Database.class;
		} else if (metaDataObject instanceof Table) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").in(\"__Schema.tables\").toList()";
			type = Schema.class;
		}
		List<MetaDataObjectReference> parentList = search(queryString);
		if (parentList.size() == 1) {
			return InternalMetaDataUtils.getObjectList(this, parentList, type).get(0);
		} else if (parentList.size() == 0) {
			return null;
		}
		String errorMessage = MessageFormat.format("Inconsistent object reference: Metadata object with id ''{0}'' refers to more that one parent object.", metaDataObject.getReference().getId());
		throw new MetadataStoreException(errorMessage);
	}

	protected <T> List<T> getReferences(String attributeName, MetaDataObject metaDataObject, Class<T> type) {
		String queryString = "";
		String objectId = metaDataObject.getReference().getId();
		if (MetadataStoreBase.ODF_COLUMNS_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__RelationalDataSet.columns\").toList()";
		} else if (MetadataStoreBase.ODF_CONNECTIONS_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__DataStore.connections\").toList()";
		} else if (MetadataStoreBase.ODF_DATAFILEFOLDERS_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__DataFileFolder.dataFileFolders\").toList()";
		} else if (MetadataStoreBase.ODF_DATAFILES_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__DataFileFolder.dataFiles\").toList()";
		} else if (MetadataStoreBase.ODF_SCHEMAS_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__Database.schemas\").toList()";
		} else if (MetadataStoreBase.ODF_TABLES_REFERENCE.equals(attributeName)) {
			queryString = "g.V.has(\"__guid\", \"" + objectId + "\").out(\"__Schema.tables\").toList()";
		}
		return InternalMetaDataUtils.getObjectList(this, search(queryString), type);
	};

}
