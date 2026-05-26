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

/** Shape of basic-search entity headers used by Latest Entities card. */
export interface LatestEntityRowModel {
	guid?: string;
	name?: string;
	typeName?: string;
	displayText?: string;
	attributes?: {
		name?: string;
		qualifiedName?: string;
		__guid?: string;
	};
}

/**
 * Match Search name column: top-level name/displayText and attributes.name /
 * qualifiedName (Atlas may return any of these).
 */
export const resolveLatestEntityDisplayName = (
	entity: LatestEntityRowModel
): string => {
	const n =
		entity.name ??
		entity.attributes?.name ??
		entity.attributes?.qualifiedName ??
		entity.displayText ??
		entity.guid;
	return typeof n === "string" && n.trim() !== "" ? n.trim() : "Unknown";
};

export const resolveLatestEntityGuid = (
	entity: LatestEntityRowModel
): string | undefined => {
	const g =
		entity.guid ??
		(typeof entity.attributes?.__guid === "string"
			? entity.attributes.__guid
			: undefined);
	if (typeof g !== "string" || g.trim() === "") return undefined;
	return g.trim();
};

export const resolveLatestEntityTypeName = (
	entity: LatestEntityRowModel
): string => {
	const t = entity.typeName;
	return typeof t === "string" && t.trim() !== "" ? t.trim() : "Entity";
};
