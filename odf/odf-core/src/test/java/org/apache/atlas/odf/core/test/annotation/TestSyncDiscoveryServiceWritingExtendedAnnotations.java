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
package org.apache.atlas.odf.core.test.annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.atlas.odf.api.metadata.MetaDataObjectReference;
import org.apache.atlas.odf.api.metadata.models.Annotation;
import org.apache.atlas.odf.api.metadata.models.ProfilingAnnotation;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResult;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;
import org.apache.atlas.odf.api.discoveryservice.sync.SyncDiscoveryService;
import org.apache.atlas.odf.core.test.ODFTestLogger;
import org.apache.atlas.odf.json.JSONUtils;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse;

public class TestSyncDiscoveryServiceWritingExtendedAnnotations extends DiscoveryServiceBase implements SyncDiscoveryService {
	Logger logger = ODFTestLogger.get();

	public static class SyncDiscoveryServiceAnnotation extends ProfilingAnnotation {
		private String prop1 = "";
		private int prop2 = 4237;
		private MyObject prop3 = new MyObject();

		public String getProp1() {
			return prop1;
		}

		public void setProp1(String prop1) {
			this.prop1 = prop1;
		}

		public int getProp2() {
			return prop2;
		}

		public void setProp2(int prop2) {
			this.prop2 = prop2;
		}

		public MyObject getProp3() {
			return prop3;
		}

		public void setProp3(MyObject prop3) {
			this.prop3 = prop3;
		}

	}

	public static class MyObject {
		private String anotherProp = "";

		public String getAnotherProp() {
			return anotherProp;
		}

		public void setAnotherProp(String anotherProp) {
			this.anotherProp = anotherProp;
		}

		private MyOtherObject yetAnotherProp = new MyOtherObject();

		public MyOtherObject getYetAnotherProp() {
			return yetAnotherProp;
		}

		public void setYetAnotherProp(MyOtherObject yetAnotherProp) {
			this.yetAnotherProp = yetAnotherProp;
		}

	}

	public static class MyOtherObject {
		private String myOtherObjectProperty = "";

		public String getMyOtherObjectProperty() {
			return myOtherObjectProperty;
		}

		public void setMyOtherObjectProperty(String myOtherObjectProperty) {
			this.myOtherObjectProperty = myOtherObjectProperty;
		}

	}

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		try {
			MetaDataObjectReference dataSetRef = request.getDataSetContainer().getDataSet().getReference();

			List<Annotation> annotations = new ArrayList<>();
			SyncDiscoveryServiceAnnotation annotation1 = new SyncDiscoveryServiceAnnotation();
			String annotation1_prop1 = "prop1_1_" + dataSetRef.getUrl();
			annotation1.setProp1(annotation1_prop1);
			annotation1.setProp2(annotation1_prop1.hashCode());
			annotation1.setProfiledObject(dataSetRef);
			MyObject mo1 = new MyObject();
			MyOtherObject moo1 = new MyOtherObject();
			moo1.setMyOtherObjectProperty("nestedtwolevels" + annotation1_prop1);
			mo1.setYetAnotherProp(moo1);
			mo1.setAnotherProp("nested" + annotation1_prop1);
			annotation1.setProp3(mo1);
			annotations.add(annotation1);

			SyncDiscoveryServiceAnnotation annotation2 = new SyncDiscoveryServiceAnnotation();
			String annotation2_prop1 = "prop1_2_" + dataSetRef.getUrl();
			annotation2.setProp1(annotation2_prop1);
			annotation2.setProp2(annotation2_prop1.hashCode());
			annotation2.setProfiledObject(dataSetRef);
			MyObject mo2 = new MyObject();
			MyOtherObject moo2 = new MyOtherObject();
			moo2.setMyOtherObjectProperty("nestedtwolevels" + annotation2_prop1);
			mo2.setYetAnotherProp(moo2);
			mo2.setAnotherProp("nested" + annotation2_prop1);
			annotation2.setProp3(mo2);
			annotations.add(annotation2);

			DiscoveryServiceSyncResponse resp = new DiscoveryServiceSyncResponse();
			resp.setCode(DiscoveryServiceResponse.ResponseCode.OK);
			DiscoveryServiceResult dsResult = new DiscoveryServiceResult();
			dsResult.setAnnotations(annotations);
			resp.setResult(dsResult);
			resp.setDetails(this.getClass().getName() + ".runAnalysis finished OK");

			logger.info("Returning from discovery service " + this.getClass().getSimpleName() + " with result: " + JSONUtils.toJSON(resp));
			return resp;
		} catch (Exception exc) {
			throw new RuntimeException(exc);
		}
	}
}
