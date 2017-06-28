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

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for a builder that can be used to create metadata queries.
 * It uses the Java builder pattern.
 * 
 * There are two types of methods:
 * 1. Chainable methods that can be used to do simple filtering, e.g.,
 *       {@code String query = queryBuilder.objectType("DataSet").simpleCondition("name", COMPARATOR.EQUALS, "waldo").build();}
 * 2. Predefined queries that are not chainable. These are very specific queries that currently cannot be built with the chainable methods, e.g.,
 *       {@code String query = queryBuilder.connectionsForDataSet(dataSetId).build();}
 * 
 * When subclassing, note that the methods set the appropriate protected fields to null to indicate that the query was "overwritten". 
 * 
 * @See {@link MetadataStore}
 */
public abstract class MetadataQueryBuilder {

	public static enum COMPARATOR {
		EQUALS, NOT_EQUALS
	};

	protected static class Condition {
	};

	protected static class SimpleCondition extends Condition {
		public SimpleCondition(String attributeName, COMPARATOR comparator, Object value) {
			super();
			this.attributeName = attributeName;
			this.comparator = comparator;
			this.value = value;
		}

		private String attributeName;
		private COMPARATOR comparator;
		private Object value;

		public String getAttributeName() {
			return attributeName;
		}

		public COMPARATOR getComparator() {
			return comparator;
		}

		public Object getValue() {
			return value;
		}

	}

	protected String objectType;
	protected List<Condition> conditions;

	public abstract String build();

	/**
	 * Set the type of object to be queried. Names are the ones of the common model (e.g. Table, Column, etc.)
	 */
	public MetadataQueryBuilder objectType(String objectTypeName) {
		this.objectType = objectTypeName;
		return this;
	}

	/**
	 * Add a simple condition to the query. All conditions are "ANDed".
	 */
	public MetadataQueryBuilder simpleCondition(String attributeName, COMPARATOR comparator, Object value) {
		if (conditions == null) {
			conditions = new ArrayList<>();
		}
		conditions.add(new SimpleCondition(attributeName, comparator, value));
		return this;
	}

}
