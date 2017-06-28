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

public class DefaultMetadataQueryBuilder extends MetadataQueryBuilder {

	public static final String SEPARATOR_STRING = " ";
	public static final String DATASET_IDENTIFIER = "from";
	public static final String CONDITION_PREFIX = "where";
	public static final String AND_IDENTIFIER = "and";
	public static final String EQUALS_IDENTIFIER = "=";
	public static final String NOT_EQUALS_IDENTIFIER = "<>";
	public static final String QUOTE_IDENTIFIER = "'";

	@Override
	public String build() {
		if (this.objectType != null) {
			StringBuilder query = new StringBuilder(DATASET_IDENTIFIER + SEPARATOR_STRING + objectType);
			if (this.conditions != null) {
				boolean firstCondition = true;
				for (Condition condition : conditions) {
					if (condition instanceof SimpleCondition) {
						SimpleCondition simpleCond = (SimpleCondition) condition;
						if (firstCondition) {
							query.append(SEPARATOR_STRING + AND_IDENTIFIER + SEPARATOR_STRING);
						} else {
							query.append(SEPARATOR_STRING + CONDITION_PREFIX + SEPARATOR_STRING);
						}
						query.append(simpleCond.getAttributeName());
						switch (simpleCond.getComparator()) {
						case EQUALS:
							query.append(SEPARATOR_STRING + EQUALS_IDENTIFIER + SEPARATOR_STRING);
							break;
						case NOT_EQUALS:
							query.append(SEPARATOR_STRING + NOT_EQUALS_IDENTIFIER + SEPARATOR_STRING);
							break;
						default:
							throw new RuntimeException("Comparator " + simpleCond.getComparator() + " is currently not supported");
						}
						Object val = simpleCond.getValue();
						if (val instanceof MetaDataObjectReference) {
							query.append(QUOTE_IDENTIFIER + ((MetaDataObjectReference) val).getId() + QUOTE_IDENTIFIER);
						} else if (val instanceof String) {
							query.append(QUOTE_IDENTIFIER + val.toString() + QUOTE_IDENTIFIER);
						} else if (val == null) {
							query.append("null");
						} else {
							query.append(val.toString());
						}
					}
					firstCondition = false;
				}
			}
			return query.toString();
		}
		return null;
	}
}
