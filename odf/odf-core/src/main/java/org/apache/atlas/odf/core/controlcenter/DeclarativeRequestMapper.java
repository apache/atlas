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
package org.apache.atlas.odf.core.controlcenter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.text.MessageFormat;

import org.apache.atlas.odf.api.ODFFactory;
import org.apache.atlas.odf.core.Utils;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceManager;
import org.apache.atlas.odf.api.analysis.AnalysisRequest;
import org.apache.atlas.odf.api.discoveryservice.DiscoveryServiceProperties;

/**
*
* Maps a list of {@link AnnotationType} objects to a list of service ids representing concrete discovery
* services that generate the requested annotation types.
* 
* Internally, this class generates a list of all possible combinations of discovery services which may be
* used to generate the requested annotation types. The combinations are then assessed and ordered by the
* expected execution effort and the one with the least execution effort is provided. 
*
*/
public class DeclarativeRequestMapper {

	private Logger logger = Logger.getLogger(DeclarativeRequestMapper.class.getName());

	DiscoveryServiceManager dsManager = new ODFFactory().create().getDiscoveryServiceManager();
	List<DiscoveryServiceProperties> dsPropList = dsManager.getDiscoveryServicesProperties();

	private List<DiscoveryServiceSequence> discoveryServiceSequences = new ArrayList<DiscoveryServiceSequence>();

	public DeclarativeRequestMapper(AnalysisRequest request) {
		String messageText = "Generating possible discovery service sequences for annotation types {0}.";
		logger.log(Level.INFO, MessageFormat.format(messageText, request.getAnnotationTypes()));

		this.discoveryServiceSequences = calculateDiscoveryServiceSequences(request.getAnnotationTypes());
		Collections.sort(this.discoveryServiceSequences, new EffortComparator());
	}

	/**
	*
	* Represents a single discovery service sequence.
	*
	*/
	public class DiscoveryServiceSequence {
		private LinkedHashSet<String> serviceSequence;

		public DiscoveryServiceSequence() {
			this.serviceSequence = new LinkedHashSet<String>();
		}

		public DiscoveryServiceSequence(LinkedHashSet<String> serviceIds) {
			this.serviceSequence = serviceIds;
		}

		public LinkedHashSet<String> getServiceSequence() {
			return this.serviceSequence;
		}

		public List<String> getServiceSequenceAsList() {
			return new ArrayList<String>(this.serviceSequence);
		}

		@Override
		public boolean equals(Object obj) {
			if ((obj == null) || !(obj instanceof DiscoveryServiceSequence)) {
				return false;
			}
			return this.getServiceSequence().equals(((DiscoveryServiceSequence) obj).getServiceSequence());
		}

		// Overriding hashCode method to ensure proper results of equals() method
		// (See of http://www.javaranch.com/journal/2002/10/equalhash.html)
		@Override
		public int hashCode() {
			return Utils.joinStrings(new ArrayList<String>(this.serviceSequence), ',').hashCode();
		}
	}

	/**
	*
	* Internal class that estimates the effort for executing a sequence of discovery services.
	* Should be extended to take runtime statistics into account. 
	*
	*/
	private class EffortComparator implements Comparator<DiscoveryServiceSequence> {
		public int compare(DiscoveryServiceSequence da1, DiscoveryServiceSequence da2) {
			if (da1.getServiceSequence().size() < da2.getServiceSequence().size()) {
				return -1;
			} else if (da1.getServiceSequence().size() > da2.getServiceSequence().size()) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	/**
	 * Returns the calculated list of discovery service sequences ordered by the execution effort,
	 * starting with the sequence that is supposed to cause the minimum execution effort.
	 *
	 * @return List of discovery service sequences
	 */
	public List<DiscoveryServiceSequence> getDiscoveryServiceSequences() {
		return this.discoveryServiceSequences;
	}

	/**
	 * Returns recommended discovery service sequence, i.e. the one that is supposed to cause the
	 * minimum execution effort.
	 *
	 * @return Discovery service sequence
	 */
	public List<String> getRecommendedDiscoveryServiceSequence() {
		if (!getDiscoveryServiceSequences().isEmpty()) {
			return new ArrayList<String>(this.discoveryServiceSequences.get(0).getServiceSequence());
		} else {
			return null;
		}
	}

	/**
	 * Remove all discovery service sequences that contain a specific service id. Use this method
	 * to update the list of discovery service sequences after a specific discovery service has
	 * failed and should not be used any more.
	 *
	 * @param serviceId Id of discovery service to be removed
	 * @return Discovery service sequence
	 */
	public boolean removeDiscoveryServiceSequences(String serviceId) {
		boolean serviceRemoved = false;
		List<DiscoveryServiceSequence> updatedList = new ArrayList<DiscoveryServiceSequence>();
		updatedList.addAll(this.discoveryServiceSequences);
		for (DiscoveryServiceSequence sequence : this.discoveryServiceSequences) {
			if (sequence.getServiceSequence().contains(serviceId)) {
				updatedList.remove(sequence);
				serviceRemoved = true;
			}
		}
		this.discoveryServiceSequences = updatedList;
		return serviceRemoved ? true : false;
	}

	/**
	 * Internal method that determines all possible sequences of discovery services which could be used
	 * to generate the requested annotation type. Using recursion, all levels of prerequisites are taken
	 * into account.
	 *
	 * @param annotationType Annotation type to be generated
	 * @return List of discovery service sequences that generate the requested annotation type
	 */
	private List<DiscoveryServiceSequence> getDiscoveryServiceSequencesForAnnotationType(String annotationType) {
		List<DiscoveryServiceSequence> result = new ArrayList<DiscoveryServiceSequence>();
		for (DiscoveryServiceProperties dsProps : this.dsPropList) {
			if ((dsProps.getResultingAnnotationTypes() != null) && dsProps.getResultingAnnotationTypes().contains(annotationType)) {
				DiscoveryServiceSequence da = new DiscoveryServiceSequence();
				da.getServiceSequence().add(dsProps.getId());
				List<DiscoveryServiceSequence> discoveryApproachesForService = new ArrayList<DiscoveryServiceSequence>();
				discoveryApproachesForService.add(da);

				// If there are prerequisite annotation types, also merge their services into the result
				if ((dsProps.getPrerequisiteAnnotationTypes() != null)
						&& !dsProps.getPrerequisiteAnnotationTypes().isEmpty()) {
					discoveryApproachesForService = combineDiscoveryServiceSequences(
							calculateDiscoveryServiceSequences(dsProps.getPrerequisiteAnnotationTypes()),
							discoveryApproachesForService);
					;
				}
				logger.log(Level.INFO, "Discovery appoaches for annotationType " + annotationType + ":");
				for (DeclarativeRequestMapper.DiscoveryServiceSequence discoveryApproach : discoveryApproachesForService) {
					logger.log(Level.INFO,
							Utils.joinStrings(new ArrayList<String>(discoveryApproach.getServiceSequence()), ','));
				}

				result.addAll(discoveryApproachesForService);
			}
		}
		return result;
	}

	/**
	 * Internal method that combines two lists of discovery service sequences by generating all possible
	 * combinations of the entries of both lists. The methods avoids duplicate services in each sequence
	 * and duplicate sequences in the resulting list.
	 *
	 * @param originalSequences Original list of discovery service sequences
	 * @param additionalSequences Second list discovery service sequences
	 * @return Combined list of discovery service sequences
	 */
	private List<DiscoveryServiceSequence> combineDiscoveryServiceSequences(List<DiscoveryServiceSequence> originalSequences, List<DiscoveryServiceSequence> additionalSequences) {
		// Example scenario for combining service sequences:
		//
		// Lets assume a service S that generates two annotation types AT1 and AT2 and S has prerequisite
		// annotation type AT_P. There are two services P1 and P2 creating annotation type AT_P.
		// The possible service sequences for generating annotation type AT1 are "P1, S" and "P2, S", same for AT2.
		//
		// When requesting a set of annotation types AT1 and AT2, this will result in the following four combinations
		// which contain several redundancies:
		// "P1, S, P1, S", "P1, S, P2, S", "P2, S, P1, S", "P2, S, P2, S"
		// 
		// This method uses three ways of removing redundancies:
		//
		// 1. Given that class DiscoveryServiceSequence internally uses LinkedHashSet, duplicate services are removed from the
		// service sequences, resulting in: "P1, S", "P1, S, P2", "P2, S, P1", "P2, S"
		//
		// 2. Service sequences are only merged if the last service of the additional sequence is not already part of the original
		// one which results in: "P1, S", "P1, S", "P2, S", "P2, S"
		// 
		// 3. Duplicate sequences are ignored, resulting in: "P1, S", "P2, S" which is the final result.  

		List<DiscoveryServiceSequence> discoveryApproaches = new ArrayList<DiscoveryServiceSequence>();
		for (DiscoveryServiceSequence da1 : originalSequences) {
			for (DiscoveryServiceSequence da2 : additionalSequences) {
				DiscoveryServiceSequence da = new DiscoveryServiceSequence();
				da.getServiceSequence().addAll(da1.getServiceSequence());

				// Add the second list only if its last serviceId is not already part of the first list
				// (Otherwise unnecessary prerequisite services might be added, because the 2nd list may use different ones)
				if (!da1.getServiceSequence().contains(da2.getServiceSequenceAsList().get(da2.getServiceSequenceAsList().size() - 1))) {
					da.getServiceSequence().addAll(da2.getServiceSequence());
				}

				// Avoid duplicate entries (uses DiscoveryServiceSequence.equals() method)
				if (!discoveryApproaches.contains(da)) {
					discoveryApproaches.add(da);
				}
			}
		}
		return discoveryApproaches;
	}

	/**
	 * Internal method that determines all possible sequences of discovery services which could be used
	 * to generate a set of requested annotation types.
	 *
	 * Each discovery service creates one or multiple annotation types and may have prerequisite annotation types.
	 * As there may be multiple services creating the same annotation type (maybe by using different prerequisite
	 * annotation types), this may result in a complex dependencies. Using recursion, this method iterates through 
	 * all the dependencies in order to calculate a list of all possible sequences of discovery services that could
	 * be used to calculate the requested annotation types.
	 * 
	 * @param annotationTypes List of annotation types to be generated
	 * @return List of discovery service sequences that generate the requested annotation types
	 */
	private List<DiscoveryServiceSequence> calculateDiscoveryServiceSequences(List<String> annotationTypes) {
		List<DiscoveryServiceSequence> result = null;

		for (String currentType : annotationTypes) {
			// Calculate discovery sequences for current annotation type
			List<DiscoveryServiceSequence> additionalDiscoveryApproaches = getDiscoveryServiceSequencesForAnnotationType(currentType);
			if (result == null) {
				result = additionalDiscoveryApproaches;
			} else {
				// Merge with discovery sequences determined for the previous annotation types in the list 
				result = combineDiscoveryServiceSequences(result, additionalDiscoveryApproaches);
			}
		}
		return result;
	}
}
