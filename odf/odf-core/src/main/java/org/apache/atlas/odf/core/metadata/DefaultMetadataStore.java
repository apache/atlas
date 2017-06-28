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
package org.apache.atlas.odf.core.metadata;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.AnnotationPropagator;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.atlas.odf.api.metadata.StoredMetaDataObject;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.RelationshipAnnotation;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import org.apache.atlas.odf.api.annotation.AnnotationStore;
import org.apache.atlas.odf.api.metadata.DefaultMetadataQueryBuilder;
import org.apache.atlas.odf.api.metadata.InternalMetaDataUtils;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStoreException;
import org.apache.atlas.odf.api.metadata.models.ClassificationAnnotation;
import org.apache.atlas.odf.api.metadata.models.ConnectionInfo;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.json.JSONUtils;

/**
 * In-memory implementation of MetadataStore interface to be used for testing as
 * well as for single-node ODF deployments. Uses static HashMaps for storing the
 * metadata types and objects.
 * 
 * 
 */
public class DefaultMetadataStore extends WritableMetadataStoreBase implements WritableMetadataStore {
	private Logger logger = Logger.getLogger(DefaultMetadataStore.class.getName());

	private static final String METADATA_STORE_ID = "ODF_LOCAL_METADATA_STORE";
	private static final String STORE_PROPERTY_TYPE = "default";
	private static final String STORE_PROPERTY_DESCRIPTION = "ODF local metadata store";

	private static HashMap<String, String> typeStore;
	private static HashMap<String, StoredMetaDataObject> objectStore;
	protected LinkedHashMap<String, StoredMetaDataObject> stagedObjects = new LinkedHashMap<String, StoredMetaDataObject>();
	private static boolean isInitialized = false;
	protected static Object accessLock = new Object();
	static Object initializationLock = new Object();

	public DefaultMetadataStore() {
		synchronized (initializationLock) {
			if (!isInitialized) {
				isInitialized = true;
				this.resetAllData();
			}
		}
	}

	protected WritableMetadataStore getMetadataStore() {
		return this;
	}

	protected Object getAccessLock() {
		return accessLock;
	}

	protected HashMap<String, StoredMetaDataObject> getObjects() {
		return objectStore;
	}

	protected LinkedHashMap<String, StoredMetaDataObject> getStagedObjects() {
		return stagedObjects;
	}

	@Override
    public ConnectionInfo getConnectionInfo(MetaDataObject informationAsset) {
    	synchronized(accessLock) {
    		return WritableMetadataStoreUtils.getConnectionInfo(this, informationAsset);
    	}
    };

	@Override
	public void resetAllData() {
		logger.log(Level.INFO, "Resetting all data in metadata store.");
		synchronized (accessLock) {
			typeStore = new HashMap<String, String>();
			objectStore = new HashMap<String, StoredMetaDataObject>();
			createTypes(WritableMetadataStoreUtils.getBaseTypes());
		}
	}

	@Override
	public Properties getProperties() {
		Properties props = new Properties();
		props.put(MetadataStore.STORE_PROPERTY_DESCRIPTION, STORE_PROPERTY_DESCRIPTION);
		props.put(MetadataStore.STORE_PROPERTY_TYPE, STORE_PROPERTY_TYPE);
		props.put(STORE_PROPERTY_ID, METADATA_STORE_ID);
		return props;
	}

	@Override
	public String getRepositoryId() {
		return METADATA_STORE_ID;
	}

	@Override
	public List<MetaDataObjectReference> search(String query) {
		if ((query == null) || query.isEmpty()) {
			throw new MetadataStoreException("The search term cannot be null or empty.");
		}
		logger.log(Level.INFO, MessageFormat.format("Processing query \"{0}\".", query));
		synchronized (accessLock) {
			LinkedList<String> queryElements = new LinkedList<String>();
			for (String el : query.split(DefaultMetadataQueryBuilder.SEPARATOR_STRING)) {
				queryElements.add(el);
			}
			List<MetaDataObjectReference> result = new ArrayList<MetaDataObjectReference>();
			String firstOperator = queryElements.removeFirst();

			if (firstOperator.equals(DefaultMetadataQueryBuilder.DATASET_IDENTIFIER)) {
				String requestedObjectType = queryElements.removeFirst();
				for (StoredMetaDataObject currentInternalObject : getObjects().values()) {
					MetaDataObject currentObject = currentInternalObject.getMetaDataObject();
					String currentObjectType = getObjectType(currentObject);
					try {
						if (isSubTypeOf(requestedObjectType, currentObjectType)
								&& isConditionMet(currentObject, queryElements)) {
							result.add(currentObject.getReference());
						}
					} catch (IllegalArgumentException | IllegalAccessException e) {
						throw new MetadataStoreException(
								MessageFormat.format("Error processing \"{0}\" clause of query.",
										DefaultMetadataQueryBuilder.DATASET_IDENTIFIER));
					}
				}
				return result;
			} else {
				throw new MetadataStoreException(MessageFormat.format("Query ''{0}'' is not valid.", query));
			}
		}
	}

	@Override
	public void createSampleData() {
		logger.log(Level.INFO, "Creating sample data in metadata store.");
		SampleDataHelper.copySampleFiles();
		WritableMetadataStoreUtils.createSampleDataObjects(this);
	}

	@Override
	public AnnotationPropagator getAnnotationPropagator() {
		return new AnnotationPropagator() {

			@Override
			public void propagateAnnotations(AnnotationStore as, String requestId) {
				List<Annotation> annotations = as.getAnnotations(null, requestId);
				for (Annotation annot : annotations) {
					ensureAnnotationTypeExists(annot);
					annot.setReference(null); // Set reference to null because a new reference will be generated by the metadata store
					getMetadataStore().createObject(annot);
					commit();
				}
			}
		};
	}

	/**
	 * Internal helper that creates a list of types in the metadata store.
	 *
	 * @param typeList List of types to be created
	 */
	private void createTypes(List<Class<?>> typeList) {
		synchronized (accessLock) {
			for (Class<?> type : typeList) {
				if (!typeStore.containsKey(type.getSimpleName())) {
					logger.log(Level.INFO,
							MessageFormat.format("Creating new type \"{0}\" in metadata store.", type.getSimpleName()));
					typeStore.put(type.getSimpleName(), type.getSuperclass().getSimpleName());
				} else {
					throw new MetadataStoreException(MessageFormat.format(
							"A type with the name \"{0}\" already exists in this metadata store.", type.getName()));
				}
			}
		}
	};

	/**
	 * Internal helper that returns the type name of a given metadata object.
	 *
	 * @param mdo Metadata object
	 * @return Type name 
	 */
	protected String getObjectType(MetaDataObject mdo) {
		if (mdo instanceof Annotation) {
			// Important when using the MetadataStore as an AnnotationStore
			return ((Annotation) mdo).getAnnotationType();
		} else {
			return mdo.getClass().getSimpleName();
		}
	}

	/**
	 * Internal helper that checks if a type is a sub type of another type 
	 *
	 * @param subTypeName Name of the type that is supposed to be the sub type
	 * @param parentTypeName Name of the type that is supposed to be the parent type
	 */
	private boolean isSubTypeOf(String subTypeName, String parentTypeName) {
		if (subTypeName.equals(parentTypeName)) {
			return true;
		}
		if (typeStore.get(parentTypeName) != null) {
			String parent = typeStore.get(parentTypeName);
			if ((parent != null) && (!parent.equals(parentTypeName))) {
				if (isSubTypeOf(subTypeName, parent)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Internal helper that checks if the attributes of a given metadata object meet a given condition. 
	 *
	 * @param mdo Metadata object
	 * @param condition List of tokens that make up the condition phrase
	 */
	private boolean isConditionMet(MetaDataObject mdo, LinkedList<String> condition)
			throws IllegalArgumentException, IllegalAccessException {
		if (condition.isEmpty()) {
			return true;
		}
		LinkedList<String> clonedCondition = new LinkedList<String>();
		clonedCondition.addAll(condition);
		try {
			JSONObject mdoJson = JSONUtils.toJSONObject(mdo);
			logger.log(Level.FINER, MessageFormat.format("Evaluating object \"{0}\".", mdoJson));
			while (clonedCondition.size() >= 4) {
				// Each condition clause consists of four elements, e.g. "where
				// name = 'BankClientsShort'" or "and name = 'BankClientsShort'"
				String operator = clonedCondition.removeFirst();
				String attribute = clonedCondition.removeFirst();
				String comparator = clonedCondition.removeFirst();
				String expectedValueWithQuotes = clonedCondition.removeFirst();
				while ((!expectedValueWithQuotes.endsWith(DefaultMetadataQueryBuilder.QUOTE_IDENTIFIER)) && (clonedCondition.size() != 0)) {
					expectedValueWithQuotes = expectedValueWithQuotes + DefaultMetadataQueryBuilder.SEPARATOR_STRING + clonedCondition.removeFirst();
				}
				if (operator.equals(DefaultMetadataQueryBuilder.CONDITION_PREFIX)
						|| operator.equals(DefaultMetadataQueryBuilder.AND_IDENTIFIER)) {
					if (mdoJson.containsKey(attribute)) {
						String actualValue = (String) mdoJson.get(attribute) != null ? mdoJson.get(attribute).toString() : null;
						if (comparator.equals(DefaultMetadataQueryBuilder.EQUALS_IDENTIFIER)) {
							if (!expectedValueWithQuotes.equals(DefaultMetadataQueryBuilder.QUOTE_IDENTIFIER + actualValue + DefaultMetadataQueryBuilder.QUOTE_IDENTIFIER)) {
								// Condition is not met
								return false;
							}
						} else if (comparator.equals(DefaultMetadataQueryBuilder.NOT_EQUALS_IDENTIFIER)) {
							if (expectedValueWithQuotes.equals(DefaultMetadataQueryBuilder.QUOTE_IDENTIFIER + actualValue + DefaultMetadataQueryBuilder.QUOTE_IDENTIFIER)) {
								// Condition is not met
								return false;
							}
						} else {
							throw new MetadataStoreException(
									MessageFormat.format("Unknown comparator \"{0}\" in query condition \"{1}\".",
											new Object[] { comparator, condition.toString() }));
						}
					} else {
						logger.log(Level.INFO,
								MessageFormat.format("The object does not contain attribute \"{0}\".", attribute));
						// Condition is not met
						return false;
					}
				} else {
					throw new MetadataStoreException(
							MessageFormat.format("Syntax error in query condition \"{0}\".", condition.toString()));
				}
			}
			if (clonedCondition.size() != 0) {
				throw new MetadataStoreException(
						MessageFormat.format("Error parsing trailing query elements \"{0}\".", clonedCondition));
			}
			// All conditions are met
			return true;
		} catch (JSONException e) {
			throw new MetadataStoreException(MessageFormat.format("Error parsing JSON object {0} in query.", mdo), e);
		}
	}

	/**
	 * Internal helper that merges the references of a staged metadata object with the references of the current metadata object
	 * stored in the metadata store. The missing references are added to the provided object in place.
	 *
	 * @param object Internal representation of a staged metadata object
	 */
	private void mergeReferenceMap(StoredMetaDataObject object) {
		HashMap<String, List<MetaDataObjectReference>> mergedObjectRefMap = new HashMap<String, List<MetaDataObjectReference>>();
		String objectId = object.getMetaDataObject().getReference().getId();
		if (getObjects().get(objectId) != null) {
			// Only merge if the object already exists in the metadata store
			HashMap<String, List<MetaDataObjectReference>> originalRefMap = getObjects().get(objectId)
					.getReferenceMap(); // Get reference map of exiting object
			HashMap<String, List<MetaDataObjectReference>> updatedObjectRefMap = object.getReferenceMap();
			for (String referenceId : updatedObjectRefMap.keySet()) {
				// Update original reference map in place
				mergedObjectRefMap.put(referenceId,
						InternalMetaDataUtils.mergeReferenceLists(originalRefMap.get(referenceId), updatedObjectRefMap.get(referenceId)));
			}
			object.setReferencesMap(mergedObjectRefMap);
		}
	}

	@Override
	public void commit() {
		synchronized (accessLock) {
			// Check if all required types exist BEFORE starting to create the
			// objects in order to avoid partial creation of objects
			for (Map.Entry<String, StoredMetaDataObject> mapEntry : this.stagedObjects.entrySet()) {
				String typeName = getObjectType(mapEntry.getValue().getMetaDataObject());
				if ((typeName == null) || !typeStore.containsKey(typeName)) {
					throw new MetadataStoreException(MessageFormat.format(
							"The type \"{0}\" of the object you are trying to create does not exist in this metadata store.",
							typeName));
				}
			}

			// Move objects from staging area into metadata store
			for (Map.Entry<String, StoredMetaDataObject> mapEntry : this.stagedObjects.entrySet()) {
				StoredMetaDataObject object = mapEntry.getValue();
				String typeName = getObjectType(mapEntry.getValue().getMetaDataObject());
				logger.log(Level.INFO,
						MessageFormat.format(
								"Creating or updating object with id ''{0}'' and type ''{1}'' in metadata store.",
								new Object[] { object.getMetaDataObject().getReference(), typeName }));
				String objectId = object.getMetaDataObject().getReference().getId();
				mergeReferenceMap(object); // Merge new object references with
											// existing object references in
											// metadata store
				getObjects().put(objectId, object);
			}

			// Clear staging area
			stagedObjects = new LinkedHashMap<String, StoredMetaDataObject>();
		}
	}

	/**
	 * Internal helper that creates a new annotation type in the internal type store if it does not yet exist.
	 *
	 * @param mds Metadata store to operate on
	 */
	private void ensureAnnotationTypeExists(Annotation annotation) {
		String annotationType = annotation.getAnnotationType();
		if (typeStore.get(annotationType) == null) {
			if (annotation instanceof ProfilingAnnotation) {
				typeStore.put(annotationType, "ProfilingAnnotation");
			} else if (annotation instanceof ClassificationAnnotation) {
				typeStore.put(annotationType, "ClassificationAnnotation");
			} else if (annotation instanceof RelationshipAnnotation) {
				typeStore.put(annotationType, "RelationshipAnnotation");
			}
		}
	}
}
