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
package org.apache.atlas.odf.core.test.discoveryservice;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.MetadataStore;
import org.apache.wink.json4j.JSONObject;
import org.junit.Assert;

import org.apache.atlas.odf.api.metadata.models.CachedMetadataStore;
import org.apache.atlas.odf.api.metadata.models.DataSet;
import org.apache.atlas.odf.api.metadata.models.MetaDataCache;
import org.apache.atlas.odf.api.metadata.models.MetaDataObject;
import org.apache.atlas.odf.api.metadata.models.RelationalDataSet;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.api.annotation.AnnotationStore;

public class TestSyncDiscoveryServiceWritingAnnotations1 extends DiscoveryServiceBase implements SyncDiscoveryService {

	static Logger logger = Logger.getLogger(TestSyncDiscoveryServiceWritingAnnotations1.class.getName());

	public static String checkMetaDataCache(DiscoveryServiceRequest request) {
		logger.info("Checking metadata cache");
		MetaDataObject mdo = request.getDataSetContainer().getDataSet();
		MetaDataCache cache = request.getDataSetContainer().getMetaDataCache();
		if (cache == null) {
			return null;
		}
		CachedMetadataStore cacheReader = new CachedMetadataStore(cache);

		if (mdo instanceof RelationalDataSet) {
			logger.info("Checking metadata cache for columns...");
			RelationalDataSet rds = (RelationalDataSet) mdo;
			Set<MetaDataObjectReference> cachedColumns = new HashSet<>();
			Set<MetaDataObjectReference> actualColumns = new HashSet<>();
			for (MetaDataObject col : cacheReader.getColumns(rds)) {
				cachedColumns.add(col.getReference());
			}
			MetadataStore mds = new ODFFactory().create().getMetadataStore();
			for (MetaDataObject col : mds.getColumns(rds)) {
				actualColumns.add(col.getReference());
			}
			Assert.assertTrue("Columns missing from metadata cache.", cachedColumns.containsAll(actualColumns));
			Assert.assertTrue("Too many columns in metadata cache.", actualColumns.containsAll(cachedColumns));
		}
		return null;
	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		logger.info("Analysis started on sync test service with annotations ");
		String errorMessage = createAnnotations( //
				request.getDataSetContainer().getDataSet().getReference(), //
				(String) request.getAdditionalProperties().get(REQUEST_PROPERTY_CORRELATION_ID), //
				metadataStore, //
				annotationStore);
		if (errorMessage == null) {
			errorMessage = checkMetaDataCache(request);
		}
		DiscoveryServiceSyncResponse resp = new DiscoveryServiceSyncResponse();
		if (errorMessage == null) {
			resp.setCode(DiscoveryServiceResponse.ResponseCode.OK);
			resp.setDetails("Annotations created successfully");
		} else {
			resp.setCode(DiscoveryServiceResponse.ResponseCode.UNKNOWN_ERROR);
			resp.setDetails(errorMessage);
		}
		logger.info("Analysis finished on sync test service with annotations ");

		return resp;
	}

	public static final String REQUEST_PROPERTY_CORRELATION_ID = "REQUEST_PROPERTY_CORRELATION_ID";

	static final String ANNOTATION_TYPE = "AnnotationType-" + TestSyncDiscoveryServiceWritingAnnotations1.class.getSimpleName();
	static final String JSON_ATTRIBUTE = "Attribute-" + TestSyncDiscoveryServiceWritingAnnotations1.class.getSimpleName();
	static final String JSON_VALUE = "Value-" + TestSyncDiscoveryServiceWritingAnnotations1.class.getSimpleName();

	public static int getNumberOfAnnotations() {
		return 3;
	}

	public static String[] getPropsOfNthAnnotation(int i) {
		return new String[] { ANNOTATION_TYPE + i, JSON_ATTRIBUTE + i, JSON_VALUE + i };
	}

	public static String createAnnotations(MetaDataObjectReference dataSetRef, String correlationId, MetadataStore mds, AnnotationStore as) {
		try {
			TestSyncDiscoveryServiceWritingAnnotations1.logger.info("Analysis will run on data set ref: " + dataSetRef);
			MetaDataObject dataSet = mds.retrieve(dataSetRef);

			String errorMessage = null;
			if (dataSet == null) {
				errorMessage = "Data set with id " + dataSetRef + " could not be retrieved";
				TestSyncDiscoveryServiceWritingAnnotations1.logger.severe(errorMessage);
				return errorMessage;
			}

			if (!(dataSet instanceof DataSet)) {
				errorMessage = "Object with id " + dataSetRef + " is not a data set";
				TestSyncDiscoveryServiceWritingAnnotations1.logger.severe(errorMessage);
				return errorMessage;
			}

			// add some annotations
			for (int i = 0; i < getNumberOfAnnotations(); i++) {
				String[] annotValues = getPropsOfNthAnnotation(i);
				ProfilingAnnotation annotation1 = new ProfilingAnnotation();
				annotation1.setProfiledObject(dataSetRef);
				annotation1.setAnnotationType(annotValues[0]);
				JSONObject jo1 = new JSONObject();
				jo1.put(annotValues[1], annotValues[2]);
				jo1.put(REQUEST_PROPERTY_CORRELATION_ID, correlationId);
				annotation1.setJsonProperties(jo1.write());

// PG: dynamic type creation disabled (types are already created statically)
//				mds.createAnnotationTypesFromPrototypes(Collections.singletonList(annotation1));
				MetaDataObjectReference resultRef1 = as.store(annotation1);
				if (resultRef1 == null) {
					throw new RuntimeException("Annotation object " + i + " could not be created");
				}
			}

			TestSyncDiscoveryServiceWritingAnnotations1.logger.info("Discovery service " + TestSyncDiscoveryServiceWritingAnnotations1.class.getSimpleName() + "created annotations successfully");
		} catch (Throwable exc) {
			exc.printStackTrace();
			TestSyncDiscoveryServiceWritingAnnotations1.logger.log(Level.WARNING, TestSyncDiscoveryServiceWritingAnnotations1.class.getSimpleName() + " has failed", exc);
			return "Failed: " + Utils.getExceptionAsString(exc);
		}
		return null;
	}

}
