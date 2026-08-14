/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { getEntityWithRelationships } from "@api/apiMethods/detailpageApiMethod";

export interface EntityPayload {
	guid?: string;
	typeName?: string;
	relationshipAttributes?: Record<string, unknown>;
	customAttributes?: Record<string, string>;
	[key: string]: unknown;
}

/**
 * Returns true when a relationship attribute value is absent or incomplete
 * (mirrors classic EntityUserDefineView.isRelationshipAttrValueMissing).
 */
export const isRelationshipAttrValueMissing = (val: unknown): boolean => {
	if (val === undefined || val === null) {
		return true;
	}
	if (Array.isArray(val)) {
		return val.length === 0;
	}
	if (typeof val === "object") {
		return !(val as { guid?: string }).guid;
	}
	return false;
};

/**
 * Fills missing relationshipAttributes on entityJson from a full entity GET
 * (ignoreRelationships=false). Preserves existing meanings when present.
 */
export const mergeMissingRelationshipAttributes = (
	entityJson: EntityPayload,
	fullEntityRelationshipAttributes: Record<string, unknown> | undefined,
	preserveMeanings = false
): void => {
	entityJson.relationshipAttributes = entityJson.relationshipAttributes || {};
	const hadMeanings =
		Array.isArray(entityJson.relationshipAttributes.meanings) &&
		entityJson.relationshipAttributes.meanings.length > 0;

	if (!fullEntityRelationshipAttributes) {
		return;
	}

	Object.entries(fullEntityRelationshipAttributes).forEach(([key, val]) => {
		if (key === "meanings" && (preserveMeanings || hadMeanings)) {
			return;
		}
		if (
			!isRelationshipAttrValueMissing(entityJson.relationshipAttributes![key])
		) {
			return;
		}
		entityJson.relationshipAttributes![key] = val;
	});
};

/**
 * Detail page GET uses ignoreRelationships=true. Before POST /v2/entity for
 * user-defined properties, fetch full entity and merge mandatory relationship
 * refs (e.g. hive_column.table) into the save payload.
 */
export const enrichEntityPayloadForRelationshipSave = async (
	entityJson: EntityPayload
): Promise<void> => {
	const entityGuid = entityJson.guid;
	if (!entityGuid) {
		return;
	}

	entityJson.relationshipAttributes = entityJson.relationshipAttributes || {};
	const srcMeanings = entityJson.relationshipAttributes.meanings;
	const hadMeanings = Array.isArray(srcMeanings) && srcMeanings.length > 0;
	if (hadMeanings) {
		entityJson.relationshipAttributes.meanings = srcMeanings;
	}

	try {
		const response = await getEntityWithRelationships(entityGuid);
		const fullEntity = response?.data?.entity;
		if (fullEntity?.relationshipAttributes) {
			mergeMissingRelationshipAttributes(
				entityJson,
				fullEntity.relationshipAttributes,
				hadMeanings
			);
		}
	} catch {
		// Save proceeds with existing attrs; server may reject if mandatory refs
		// are still missing (same as classic UI when full GET fails).
	}
};
