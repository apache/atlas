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

import { jsonParse } from "@utils/Utils";

/** TYPE_DEF_* audit params that use the same detail modal as the Administrator Audits tab */
export const TYPE_DEF_DETAIL_MODAL_PARAMS = new Set([
	"ENUM",
	"ENTITY",
	"STRUCT",
	"RELATIONSHIP",
]);

export interface AuditRecordLike {
	operation?: string;
	params?: string;
	result?: string;
}

/**
 * Pulls the full typedef object from an audit `result` JSON for ENUM / ENTITY /
 * STRUCT / RELATIONSHIP rows (matches expanded audit row click payload).
 */
export const extractTypeDefDetailObject = (
	record: AuditRecordLike,
	resolvedName: string,
	resolvedGuid: string
): Record<string, unknown> | null => {
	const op = record.operation;
	if (
		op !== "TYPE_DEF_CREATE" &&
		op !== "TYPE_DEF_UPDATE" &&
		op !== "TYPE_DEF_DELETE"
	) {
		return null;
	}
	const paramKeys = (record.params ?? "")
		.split(",")
		.map((k) => k.trim())
		.filter(Boolean);

	try {
		const resultObj =
			typeof record.result === "string" ? jsonParse(record.result) : record.result;
		if (!resultObj || typeof resultObj !== "object") return null;

		const keysToScan =
			paramKeys.length > 0 ? paramKeys : Object.keys(resultObj as object);

		for (const key of keysToScan) {
			if (!TYPE_DEF_DETAIL_MODAL_PARAMS.has(key)) continue;
			const list = (resultObj as Record<string, unknown>)[key];
			if (!Array.isArray(list) || list.length === 0) continue;
			const match = list.find((item: { name?: string; guid?: string }) => {
				if (!item || typeof item !== "object") return false;
				if (resolvedName && item.name === resolvedName) return true;
				if (resolvedGuid && item.guid === resolvedGuid) return true;
				return false;
			});
			if (match && typeof match === "object") {
				return match as Record<string, unknown>;
			}
		}

		const firstKey = paramKeys.find((k) => TYPE_DEF_DETAIL_MODAL_PARAMS.has(k));
		if (firstKey) {
			const list = (resultObj as Record<string, unknown>)[firstKey];
			if (Array.isArray(list) && list[0] && typeof list[0] === "object") {
				return list[0] as Record<string, unknown>;
			}
		}
	} catch {
		/* ignore */
	}
	return null;
};
