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

public class AtlasMetadataQueryBuilder extends MetadataQueryBuilder {

	@Override
	public String build() {
		if (this.objectType != null) {
			StringBuilder query = new StringBuilder("from " + objectType);
			boolean firstCondition = true;
			if (this.conditions != null) {
				for (Condition condition : conditions) {
					if (condition instanceof SimpleCondition) {
						SimpleCondition simpleCond = (SimpleCondition) condition;
						if (firstCondition) {
							query.append(" where ");
						} else {
							query.append(" and ");
						}
						query.append(simpleCond.getAttributeName());
						switch (simpleCond.getComparator()) {
						case EQUALS:
							query.append(" = ");
							break;
						case NOT_EQUALS:
							query.append(" != ");
							break;
						default:
							throw new RuntimeException("Comparator " + simpleCond.getComparator() + " is currently not supported");
						}
						Object val = simpleCond.getValue();
						if (val instanceof MetaDataObjectReference) {
							query.append("'" + ((MetaDataObjectReference) val).getId() + "'");
						} else if (val instanceof String) {
							query.append("'" + val.toString() + "'");
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
