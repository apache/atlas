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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.settings.ODFSettings;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.MetadataStoreBase;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;
import org.apache.atlas.odf.api.metadata.UnknownMetaDataObject;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;

/**
 * This class converts ODF objects to Atlas objects / REST API requests
 * and vice versa.
 *
 *
 */
public class AtlasModelBridge {
	Logger logger = Logger.getLogger(AtlasModelBridge.class.getName());
	MetadataStore mds;

    private static final HashMap<String, String> referenceNameMap = new HashMap<String, String>();
    static {
        referenceNameMap.put(MetadataStoreBase.ODF_COLUMNS_REFERENCE, "columns");
        referenceNameMap.put(MetadataStoreBase.ODF_CONNECTIONS_REFERENCE, "connections");
        referenceNameMap.put(MetadataStoreBase.ODF_DATAFILEFOLDERS_REFERENCE, "dataFileFolders");
        referenceNameMap.put(MetadataStoreBase.ODF_DATAFILES_REFERENCE, "dataFiles");
        referenceNameMap.put(MetadataStoreBase.ODF_SCHEMAS_REFERENCE, "schemas");
        referenceNameMap.put(MetadataStoreBase.ODF_TABLES_REFERENCE, "tables");
    }

	public AtlasModelBridge(MetadataStore mds) {
		this.mds = mds;
	}

	static ODFSettings getODFConfig() {
		ODFSettings odfconf = new ODFFactory().create().getSettingsManager().getODFSettings();
		return odfconf;
	}

	private boolean isAtlasType(Object atlasJson, String className) {
		if ((atlasJson instanceof JSONObject) && ((JSONObject) atlasJson).containsKey("jsonClass")) {
			Object jsonClass = ((JSONObject) atlasJson).opt("jsonClass");
			if (jsonClass instanceof String) {
				return jsonClass.toString().equals(className);
			}
		}
		return false;
	}

	private Object convertAtlasJsonToODF(Object atlasJson, int level) throws JSONException {
		Object resultObj = atlasJson;
		if (atlasJson instanceof JSONObject) {
			JSONObject valJson = (JSONObject) atlasJson;
			if (isAtlasType(valJson, "org.apache.atlas.typesystem.json.InstanceSerialization$_Id")) {
				// JSON object is reference to other object
				String id = (String) valJson.get("id");
				resultObj = createODFReferenceJSON(level, id);
			} else if ("org.apache.atlas.typesystem.json.InstanceSerialization$_Reference".equals(valJson.opt("jsonClass"))) {
				// treat References the same as IDs
				JSONObject idObj = (JSONObject) valJson.get("id");
				String id = (String) idObj.get("id");
				resultObj = createODFReferenceJSON(level, id);
			} else if (valJson.opt("$typeName$") != null && (valJson.opt("id") instanceof String)) {
				// this only happens if the object was retrieved via the /discovery/search resource and not through /entities
				resultObj = createODFReferenceJSON(level, valJson.getString("id"));
			} else {
				JSONObject convertedJSONObject = new JSONObject();
				// always remove annotations property as it is no longer part of MetaDataObject
				valJson.remove("annotations");

				// Remove referenes to other objects because they are not attributes of the corresponding metadata objects
				for (String referenceName : referenceNameMap.values()) {
					valJson.remove(referenceName);
				}

				for (Object key : valJson.keySet()) {
					Object value = valJson.get(key);
					convertedJSONObject.put(key, convertAtlasJsonToODF(value, level + 1));
				}
				if (isAtlasType(convertedJSONObject, "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct") && (convertedJSONObject.containsKey("values"))) {
					// Remove Atlas struct object
					convertedJSONObject = (JSONObject) convertedJSONObject.get("values");
				}
				resultObj = convertedJSONObject;
			}
		} else if (atlasJson instanceof JSONArray) {
			JSONArray arr = (JSONArray) atlasJson;
			JSONArray convertedArray = new JSONArray();
			for (Object o : arr) {
				// don't increase level if traversing an array
				convertedArray.add(convertAtlasJsonToODF(o, level));
			}
			resultObj = convertedArray;
		}
		return resultObj;
	}


	private JSONObject createODFReferenceJSON(int level, String id) throws JSONException {
		JSONObject mdoref = new JSONObject();
		mdoref.put("id", id);
		mdoref.put("repositoryId", this.mds.getRepositoryId());
		mdoref.put("url", (String) this.mds.getProperties().get("atlas.url"));
		return mdoref;
	}

	public MetaDataObject createMetaDataObjectFromAtlasSearchResult(JSONObject json, int level) throws JSONException {
		String guid = (String) ((JSONObject) json.get("$id$")).get("id");
		String typeName = json.getString("$typeName$");
		json.remove("$id$");
		json.remove("$typeName$");
		MetaDataObject mdo = createMDOSkeletonForType(level, json, typeName);
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId(guid);
		ref.setRepositoryId(this.mds.getRepositoryId());
		ref.setUrl((String) this.mds.getProperties().get("atlas.url"));
		mdo.setReference(ref);
		return mdo;
	}

	public MetaDataObject createMetaDataObjectFromAtlasEntity(JSONObject json, int level) throws JSONException {
		String guid = (String) (((JSONObject) ((JSONObject) json.get("definition")).get("id")).get("id"));
		MetaDataObject mdo = createMDOSkeleton(json, level);
		MetaDataObjectReference ref = new MetaDataObjectReference();
		ref.setId(guid);
		ref.setRepositoryId(this.mds.getRepositoryId());
		ref.setUrl((String) this.mds.getProperties().get("atlas.url"));
		mdo.setReference(ref);
		return mdo;
	}

	private MetaDataObject createMDOSkeleton(JSONObject json, int level) {
		try {
			JSONObject def = (JSONObject) json.get("definition");
			if (def != null) {
				JSONObject values = (JSONObject) def.get("values");
				if (values != null) {
					String typeName = (String) def.get("typeName");
					if (typeName != null) {
						return createMDOSkeletonForType(level, values, typeName);
					}
				}
			}
		} catch (Exception exc) {
			// interpret all exceptions as "incorrect format"
			String msg = "Conversion of JSON to metadata object failed, using default";
			logger.log(Level.WARNING, msg, exc);
		}
		// fallback, create generic MDO
		return new UnknownMetaDataObject();
	}


	private MetaDataObject createMDOSkeletonForType(int level, JSONObject values, String typeName)
			throws JSONException {
		MetaDataObject result = new UnknownMetaDataObject(); // Unknown by default
		Class<?> cl;
		//TODO: Move MetaDataObject.java into models package and use this instead of DataSet
		String fullClassName = DataSet.class.getPackage().getName() + "." + typeName;
		try {
			cl = Class.forName(fullClassName);
		} catch (ClassNotFoundException e) {
			String messageText = MessageFormat.format("Cannot fine class ''{0}''.", fullClassName);
			throw new MetadataStoreException(messageText, e);
		}
		if (cl != null) {
			JSONObject modifiedValues = (JSONObject) this.convertAtlasJsonToODF(values, level);
			if (typeName.equals("ProfilingAnnotation") || typeName.equals("ClassificationAnnotation") || typeName.equals("RelationshipAnnotation")) {
				result = (MetaDataObject) JSONUtils.fromJSON(modifiedValues.write(), Annotation.class);
			} else {
				modifiedValues.put("javaClass", cl.getName());
				result = (MetaDataObject) JSONUtils.fromJSON(modifiedValues.write(), cl);
			}
		}
		return result;
	}
	@SuppressWarnings("rawtypes")
	public JSONObject createAtlasEntityJSON(StoredMetaDataObject storedObject, HashMap<String, String> typeMap, HashMap<String, MetaDataObjectReference> referenceMap, JSONObject originalAtlasJson) {
		JSONObject objectJson = null;
		MetaDataObject object = storedObject.getMetaDataObject();
		try {
			logger.log(Level.FINE, "Storing instance of " + object.getClass().getName());
			JSONObject valuesJSON = JSONUtils.toJSONObject(object); // Initialize value JSON with attributes from MetaDataObject
			valuesJSON.remove("reference"); // Remove object reference because it must not be stored in Atlas
			Class<?> cl = object.getClass();
			while (cl != MetaDataObject.class) {  // process class hierarchy up to but excluding MetaDataObject
				Field fields[] = cl.getDeclaredFields();
				for (Field f: fields) {
					f.setAccessible(true);
					try {
						Class<?> fieldClass = f.getType();
						Object fieldObject = f.get(object);
						if (fieldObject != null) {
							String fieldName = f.getName();
							if (fieldClass.getName().equals(List.class.getName())) {
								// Process reference lists which are stored in attributes of the actuals MetaDataObject, e.g. for Annotations
						        ParameterizedType stringListType = (ParameterizedType) f.getGenericType();
						        if (!((List) fieldObject).isEmpty()) {
							        Class<?> listElementClass = (Class<?>) stringListType.getActualTypeArguments()[0];
							        if (listElementClass.equals(MetaDataObjectReference.class)) {
										JSONArray referenceArray = new JSONArray();
										@SuppressWarnings("unchecked")
										List<MetaDataObjectReference> members = (List<MetaDataObjectReference>) fieldObject;
										for (MetaDataObjectReference mdor : members) {
											String referenceId = ((MetaDataObjectReference) mdor).getId();
											if (referenceMap.containsKey(referenceId)) {
												referenceArray.add(createAnnotatedObjectReference(referenceMap.get(referenceId),typeMap.get(referenceId)));
											} else {
												referenceArray.add(createAnnotatedObjectReference(mdor, mds.retrieve(mdor).getClass().getSimpleName()));
											}
										}
										valuesJSON.put(fieldName, referenceArray);
							        }
						        }
							} else if (fieldClass == MetaDataObjectReference.class) {
								// Process individual references which are stored in attributes of the actuals MetaDataObject, e.g. for Annotations
								String referenceId = ((MetaDataObjectReference) fieldObject).getId();
								if (referenceMap.containsKey(referenceId)) {
									valuesJSON.put(fieldName, createAnnotatedObjectReference(referenceMap.get(referenceId), "MetaDataObject"));
								} else {
									valuesJSON.put(fieldName, createAnnotatedObjectReference((MetaDataObjectReference) fieldObject, "MetaDataObject"));
								}
							} else {
								valuesJSON.put(fieldName, fieldObject);
							}
						}
					} catch (IllegalAccessException e) {
						throw new IOException(e);
					}
				}
				cl = cl.getSuperclass();
			}

			// Store references to other objects which are not attributes of the MetaDataObject
			for(String referenceType : mds.getReferenceTypes()) {
				String atlasReferenceName = referenceNameMap.get(referenceType);
				// Add references of original Atlas object
				JSONArray referenceArray = new JSONArray();
				if ((originalAtlasJson != null) && (originalAtlasJson.get("definition") != null)) {
					JSONObject values = originalAtlasJson.getJSONObject("definition").getJSONObject("values");
					if ((values != null) && (values.containsKey(atlasReferenceName))) {
						if (values.get(atlasReferenceName) instanceof JSONArray) {
							referenceArray = values.getJSONArray(atlasReferenceName);
						}
					}
				}
				if (storedObject.getReferenceMap().containsKey(referenceType)) {
					// Add new references for the reference type
					for (MetaDataObjectReference mdor : storedObject.getReferenceMap().get(referenceType)) {
						String referenceId = ((MetaDataObjectReference) mdor).getId();
						if (referenceMap.containsKey(referenceId)) {
							referenceArray.add(createAnnotatedObjectReference(referenceMap.get(referenceId),typeMap.get(referenceId)));
						} else {
							referenceArray.add(createAnnotatedObjectReference(mdor, mds.retrieve(mdor).getClass().getSimpleName()));
						}
					}
				}
				if (referenceArray.size() > 0) {
					valuesJSON.put(atlasReferenceName, referenceArray);
				}
			}

			String objectType;
			if (object instanceof Annotation) {
				objectType = (object instanceof ProfilingAnnotation) ? "ProfilingAnnotation" :
					(object instanceof ClassificationAnnotation) ? "ClassificationAnnotation" :
					"RelationshipAnnotation";
			} else {
				objectType = object.getClass().getSimpleName();
			}
			if (originalAtlasJson != null) {
				// When updating an existing object, its must point to the correct object id in Atlas
				objectJson = this.createAtlasEntitySkeleton(objectType, object.getReference().getId());
			} else {
				// For new objects, a generic id is used
				objectJson = this.createAtlasEntitySkeleton(objectType, null);
			}
			objectJson.put("values", valuesJSON);
		} catch (IOException exc) {
			throw new MetadataStoreException(exc);
		}
		catch (JSONException exc) {
			throw new MetadataStoreException(exc);
		}
		return objectJson;
	}

	/**
	 * Create an empty Atlas object of a certain type of a certain guid.
	 * Can be used in entity POST requests for creating or (partial) update
	 */
	private JSONObject createAtlasEntitySkeleton(String typeName, String guid) {
		try {
			JSONObject obj = null;
			obj = new JSONObject(this.getClass().getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/metadata/internal/atlas/atlas-odf-object-template.json"));
			obj.put("typeName", typeName);
			JSONObject id = (JSONObject) obj.get("id");
			id.put("typeName", typeName);
			if (guid != null) {
				id.put("id", guid);
			}
			return obj;
		} catch (JSONException exc) {
			throw new MetadataStoreException(exc);
		}
	}

	/**
	 * check if the reference belongs to this repository. Throw exception if not.
	 */
	void checkReference(MetaDataObjectReference reference) {
		if (reference == null) {
			throw new MetadataStoreException("Reference cannot be null");
		}
		if ((reference.getRepositoryId() != null) && !reference.getRepositoryId().equals(mds.getRepositoryId())) {
			throw new MetadataStoreException(
					MessageFormat.format("Repository ID ''{0}'' of reference does not match the one of this repository ''{1}''", new Object[] { reference.getRepositoryId(), mds.getRepositoryId() }));
		}
	}

	/**
	 * create an Atlas object reference that can be used whenever Atlas uses references in JSON requests
	 */
	public JSONObject createAtlasObjectReference(String guid, String typeName) {
		JSONObject ref;
		try {
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("org/apache/atlas/odf/core/metadata/internal/atlas/atlas-reference-template.json");
			ref = new JSONObject(is);
			is.close();
			ref.put("id", guid);
			ref.put("typeName", typeName);
		} catch (IOException | JSONException e) {
			// should not go wrong
			throw new RuntimeException(e);
		}
		return ref;
	}

	public JSONObject createAnnotatedObjectReference(MetaDataObjectReference annotatedObjectRef, String typeName) {
		this.checkReference(annotatedObjectRef);
		String annotatedObjectId = annotatedObjectRef.getId();
		return this.createAtlasObjectReference(annotatedObjectId, typeName);
	}

	public List<StoredMetaDataObject> getRootObjects(HashMap<String, StoredMetaDataObject> objectHashMap) {
		List<StoredMetaDataObject> rootObjectList = new ArrayList<StoredMetaDataObject>();
		for (StoredMetaDataObject object : objectHashMap.values()) {
			if (isRootObject(object, objectHashMap)) {
				rootObjectList.add(object);
			}
		}
		return rootObjectList;
	}

	private boolean isRootObject(StoredMetaDataObject object, HashMap<String, StoredMetaDataObject> objectHashMap) {
		String objectId = object.getMetaDataObject().getReference().getId();
		try {
			for (StoredMetaDataObject currentObject : objectHashMap.values()) {
				String currentObjectId = currentObject.getMetaDataObject().getReference().getId();
				if (!currentObjectId.equals(objectId)) {
					// If it is not the object itself, check whether the current object contains a reference to the object
					if (JSONUtils.toJSON(currentObject).contains(objectId)) {
						// If it does, it cannot be a root object
						return false;
					}
				}
			}
			return true;
		} catch (JSONException e) {
			throw new MetadataStoreException(MessageFormat.format("Error converting object of class ''{0}'' to JSON string", object.getClass().getName()), e);
		}
	}

}
