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
package odftutorial.discoveryservicetutorial;

import java.util.Collections;
import java.util.Date;

import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceResponse.ResponseCode;
import org.apache.atlas.odf.api.discoveryservice.SyncDiscoveryServiceBase;
import org.apache.atlas.odf.api.discoveryservice.sync.DiscoveryServiceSyncResponse;

/**
 * A simple synchronous discovery service that creates one annotation for the data set it analyzes.
 *
 */
public class ODFTutorialDiscoveryService extends SyncDiscoveryServiceBase {

	@Override
	public DiscoveryServiceSyncResponse runAnalysis(DiscoveryServiceRequest request) {
		// 1. create an annotation that annotates the data set object passed in the request
		ODFTutorialAnnotation annotation = new ODFTutorialAnnotation();
		annotation.setAnnotatedObject(request.getDataSetContainer().getDataSet().getReference());
		// set a new property called "tutorialProperty" to some string
		annotation.setTutorialProperty("Tutorial annotation was created on " + new Date());

		// 2. create a response with our annotation created above
		return createSyncResponse( //
				ResponseCode.OK, // Everything works OK 
				"Everything worked", // human-readable message
				Collections.singletonList(annotation) // new annotations
		);
	}

}
