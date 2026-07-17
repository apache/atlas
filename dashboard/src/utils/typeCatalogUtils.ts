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

import type { EntityTypeDistributionItem } from "./metricsUtils";

/** Minimal typedef row from `/api/atlas/types/typedefs/headers` (see EntitiesTree). */
export interface TypeHeaderCatalogRow {
	name: string;
	category: string;
	serviceType?: string;
	guid?: string;
}

export interface DashboardTypeCatalog {
	classificationNames: string[];
	/** Glossary-related entity typedef names (Atlas glossary model). */
	glossaryTermRelatedTypeNames: string[];
	businessMetadataNames: string[];
	/** All ENTITY typedef names (sorted). */
	entityTypeNames: string[];
}

const DEFAULT_SERVICE_TYPE = "other_types";

const countForEntityType = (
	entity: Record<string, unknown> | undefined,
	typeName: string
): { active: number; deleted: number } => {
	const active = (entity?.entityActive as Record<string, number>) || {};
	const deleted = (entity?.entityDeleted as Record<string, number>) || {};
	return {
		active: Number(active[typeName] ?? 0) || 0,
		deleted: Number(deleted[typeName] ?? 0) || 0,
	};
};

/**
 * Service-type buckets aligned with the Entities sidebar (`serviceType` on each ENTITY typedef).
 * Falls back to `{typeName}_` prefix grouping when headers are missing (e.g. hive_table → hive).
 */
export const getServiceTypeDistribution = (
	entity: Record<string, unknown> | undefined,
	typeHeaders: TypeHeaderCatalogRow[] | null | undefined,
	topN = 5
): EntityTypeDistributionItem[] => {
	const active = (entity?.entityActive as Record<string, number>) || {};
	const deleted = (entity?.entityDeleted as Record<string, number>) || {};
	const map = new Map<
		string,
		{ active: number; deleted: number; typeNames: Set<string> }
	>();

	const add = (serviceKey: string, typeName: string) => {
		const { active: a, deleted: d } = countForEntityType(entity, typeName);
		const cur = map.get(serviceKey) ?? {
			active: 0,
			deleted: 0,
			typeNames: new Set<string>(),
		};
		cur.active += a;
		cur.deleted += d;
		cur.typeNames.add(typeName);
		map.set(serviceKey, cur);
	};

	if (Array.isArray(typeHeaders) && typeHeaders.length > 0) {
		for (const row of typeHeaders) {
			if (row.category !== "ENTITY") continue;
			const st = (row.serviceType?.trim() || DEFAULT_SERVICE_TYPE) as string;
			add(st, row.name);
		}
	} else {
		const allTypes = new Set([...Object.keys(active), ...Object.keys(deleted)]);
		allTypes.forEach((typeName) => {
			const i = typeName.indexOf("_");
			const serviceKey = i === -1 ? typeName : typeName.slice(0, i);
			add(serviceKey, typeName);
		});
	}

	const rows: EntityTypeDistributionItem[] = [...map.entries()].map(
		([name, { active: a, deleted: d, typeNames }]) => ({
			name,
			active: a,
			deleted: d,
			count: a + d,
			underlyingTypeNames: [...typeNames].sort(),
		})
	);

	rows.sort((x, y) => y.count - x.count || x.name.localeCompare(y.name));

	const top = rows.slice(0, topN);
	if (top.length === topN) return top;

	const used = new Set(top.map((r) => r.name));
	const pad = rows
		.filter((r) => !used.has(r.name))
		.sort((a, b) => a.name.localeCompare(b.name));
	while (top.length < topN && pad.length > 0) {
		const next = pad.shift();
		if (next) top.push(next);
	}
	return top;
};

/** Lists derived from type headers for search / future features. */
export const buildDashboardTypeCatalog = (
	typeHeaders: TypeHeaderCatalogRow[] | null | undefined
): DashboardTypeCatalog => {
	const empty: DashboardTypeCatalog = {
		classificationNames: [],
		glossaryTermRelatedTypeNames: [],
		businessMetadataNames: [],
		entityTypeNames: [],
	};
	if (!Array.isArray(typeHeaders) || typeHeaders.length === 0) return empty;

	const classificationNames: string[] = [];
	const businessMetadataNames: string[] = [];
	const entityTypeNames: string[] = [];
	const glossaryTermRelatedTypeNames: string[] = [];

	for (const row of typeHeaders) {
		const { name, category } = row;
		if (!name) continue;
		if (category === "CLASSIFICATION") classificationNames.push(name);
		else if (category === "BUSINESS_METADATA") businessMetadataNames.push(name);
		else if (category === "ENTITY") {
			entityTypeNames.push(name);
			if (name.toLowerCase().includes("glossary")) {
				glossaryTermRelatedTypeNames.push(name);
			}
		}
	}

	const sortUnique = (arr: string[]) => [...new Set(arr)].sort((a, b) => a.localeCompare(b));

	return {
		classificationNames: sortUnique(classificationNames),
		businessMetadataNames: sortUnique(businessMetadataNames),
		entityTypeNames: sortUnique(entityTypeNames),
		glossaryTermRelatedTypeNames: sortUnique(glossaryTermRelatedTypeNames),
	};
};
