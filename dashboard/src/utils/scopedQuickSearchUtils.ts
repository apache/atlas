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

import type { TreeNode } from "@models/treeStructureType";
import type {
	ChildrenInterface,
	ServiceTypeInterface,
	TypeHeaderInterface
} from "@models/entityTreeType";
import { addOnEntities } from "./Enum";
import { customSortBy, customSortByObjectKeys, isEmpty } from "./Utils";

export type QuickSearchScope =
	| "default"
	| "entity"
	| "classification"
	| "glossary"
	| "businessMetadata";

export type ScopedOptionKind =
	| "entity-service"
	| "entity-type"
	| "classification"
	| "glossary-term"
	| "business-metadata";

export interface ScopedQuickSearchOption {
	id: string;
	title: string;
	group: string;
	kind: ScopedOptionKind;
	entityTypeName?: string;
	serviceUnderlyingTypeNames?: string[];
	classificationName?: string;
	termGuid?: string;
	glossaryGuid?: string;
	termId?: string;
	termParent?: string;
	bmGuid?: string;
}

interface MetricsEntitySnapshot {
	entityActive?: Record<string, number>;
	entityDeleted?: Record<string, number>;
}

interface GlossaryTermCategoryRow {
	parentCategoryGuid?: string;
	categoryGuid?: string;
	displayText?: string;
	termGuid?: string;
}

interface GlossaryApiRow {
	name: string;
	guid: string;
	categories?: GlossaryTermCategoryRow[];
	terms?: GlossaryTermCategoryRow[];
}

const DEFAULT_SERVICE = "other_types";

const generateServiceTypeArr = (
	entityCountArr: ServiceTypeInterface[],
	serviceType: string,
	children: ChildrenInterface,
	entityCount: number
) => {
	const existing = entityCountArr.find(
		(obj): obj is ServiceTypeInterface =>
			typeof obj === "object" && obj !== null && serviceType in obj
	);
	if (existing) {
		const bucket = (existing as Record<string, {
			children: ChildrenInterface[];
			totalCount: number;
		}>)[serviceType];
		if (bucket) {
			bucket.children.push(children);
			bucket.totalCount += entityCount;
		}
	} else {
		entityCountArr.push({
			[serviceType]: {
				children: [children],
				name: serviceType,
				totalCount: entityCount
			}
		} as ServiceTypeInterface);
	}
};

const pushRootEntityToTree = (
	entities: ServiceTypeInterface[],
	allEntityCategory: string | undefined
) => {
	const rootEntityChildren: ChildrenInterface = {
		gType: "Entity",
		guid: addOnEntities[0],
		id: addOnEntities[0],
		name: addOnEntities[0],
		type: allEntityCategory,
		text: addOnEntities[0]
	};
	const hasOther = entities.some((obj) => obj["other_types"] !== undefined);
	if (hasOther) {
		const idx = entities.findIndex((obj) => "other_types" in obj);
		(entities[idx] as ServiceTypeInterface)["other_types"].children.push(
			rootEntityChildren
		);
	} else {
		entities.push({
			other_types: {
				name: "other_types",
				children: [rootEntityChildren],
				totalCount: 0
			}
		} as ServiceTypeInterface);
	}
	return entities;
};

/**
 * Mirrors {@link EntitiesTree} group view (service type → entity typedefs).
 */
export const buildEntitiesTreeForQuickSearch = (
	typeHeaderData: TypeHeaderInterface[] | null | undefined,
	metricsEntity: MetricsEntitySnapshot | null | undefined,
	allEntityCategory: string | undefined
): TreeNode[] => {
	if (!Array.isArray(typeHeaderData) || !typeHeaderData.length || !metricsEntity) {
		return [];
	}
	const active = metricsEntity.entityActive || {};
	const deleted = metricsEntity.entityDeleted || {};
	const newArr: ServiceTypeInterface[] = [];

	typeHeaderData.forEach((entity) => {
		let { serviceType = DEFAULT_SERVICE, category, name, guid } = entity;
		if (category !== "ENTITY") return;
		const entityCount =
			Number(active[name] ?? 0) + Number(deleted[name] ?? 0);
		const modelName = entityCount ? `${name} (${entityCount})` : name;
		const children: ChildrenInterface = {
			text: modelName,
			name,
			type: category,
			gType: "Entity",
			guid,
			id: guid
		};
		generateServiceTypeArr(newArr, serviceType, children, entityCount);
	});

	pushRootEntityToTree(newArr, allEntityCategory);

	const child = (childs: ChildrenInterface[]) => {
		if (!childs?.length) return [];
		return customSortBy(
			childs.map((obj) => ({
				id: obj.name as string,
				label: obj.text as string,
				types: "child"
			})),
			["label"]
		);
	};

	const sorted = customSortByObjectKeys(newArr);
	return sorted.map((entity: ServiceTypeInterface) => {
		const key = Object.keys(entity)[0];
		const entityData = entity[key];
		return {
			id: entityData.name,
			label:
				entityData.totalCount === 0
					? entityData.name
					: `${entityData.name} (${entityData.totalCount})`,
			children: child(entityData.children),
			types: "parent"
		};
	});
};

const filterTreeNodes = (treeData: TreeNode[], searchTerm: string): TreeNode[] => {
	const q = searchTerm.trim().toLowerCase();
	if (!q) return treeData;
	return treeData
		.filter(
			(node) =>
				node.label?.toLowerCase().includes(q) ||
				node.children?.some((child) => child.label?.toLowerCase().includes(q))
		)
		.map((node) => {
			const parentMatches = node.label?.toLowerCase().includes(q) ?? false;
			const kids = node.children?.length
				? parentMatches
					? node.children
					: node.children.filter((c) => c.label?.toLowerCase().includes(q))
				: undefined;
			return { ...node, children: kids };
		});
};

export const entityTreeToScopedOptions = (
	tree: TreeNode[],
	searchTerm: string
): ScopedQuickSearchOption[] => {
	const filtered = filterTreeNodes(tree, searchTerm);
	const out: ScopedQuickSearchOption[] = [];
	for (const parent of filtered) {
		const underlying =
			parent.children?.map((c) => c.id).filter(Boolean) ?? [];
		out.push({
			id: `svc:${parent.id}`,
			title: parent.label,
			group: "Entity",
			kind: "entity-service",
			serviceUnderlyingTypeNames: underlying
		});
		for (const child of parent.children ?? []) {
			out.push({
				id: `type:${child.id}`,
				title: child.label,
				group: "Entity",
				kind: "entity-type",
				entityTypeName: child.id
			});
		}
	}
	return out;
};

interface ClassificationDefRow {
	name: string;
	subTypes: string[];
	guid: string;
	superTypes: string[];
}

/** Depth-first classification list (same traversal as sidebar tree). */
export const buildClassificationScopedOptions = (
	classificationData: { classificationDefs: ClassificationDefRow[] } | null,
	tagEntities: Record<string, number> | undefined,
	searchTerm: string
): ScopedQuickSearchOption[] => {
	if (!classificationData?.classificationDefs?.length) return [];
	const defs = classificationData.classificationDefs;
	const byName = new Map(defs.map((d) => [d.name, d]));
	const q = searchTerm.trim().toLowerCase();
	const out: ScopedQuickSearchOption[] = [];
	const seen = new Set<string>();

	const visit = (typeName: string) => {
		if (seen.has(typeName)) return;
		seen.add(typeName);
		const def = byName.get(typeName);
		if (!def) return;
		const count = tagEntities?.[typeName];
		const label =
			count !== undefined ? `${typeName} (${count})` : typeName;
		if (!q || label.toLowerCase().includes(q) || typeName.toLowerCase().includes(q)) {
			out.push({
				id: `cls:${def.guid}:${typeName}`,
				title: label,
				group: "Classification",
				kind: "classification",
				classificationName: typeName
			});
		}
		(def.subTypes ?? []).forEach((st) => visit(st));
	};

	defs
		.filter((d) => isEmpty(d.superTypes))
		.forEach((d) => visit(d.name));

	return customSortBy(out, ["title"]);
};

/** Build glossary tree for “terms” mode (see {@link GlossaryTree} glossaryType true). */
export const buildGlossaryTermsTreeForQuickSearch = (
	glossaryData: GlossaryApiRow[] | null | undefined
): TreeNode[] => {
	if (!glossaryData?.length) return [];
	const glossaryType = true;

	const toServiceRows: Record<string, {
		name: string;
		children: ChildrenInterface[];
		id: string;
		types: string;
		parent: string;
		guid: string;
	}>[] = glossaryData.map((glossary) => {
		const categoryRelation =
			glossary.categories?.filter((o) => o.parentCategoryGuid !== undefined) ??
			[];

		const getChildren = (glossaries: {
			children: NonNullable<GlossaryApiRow["terms"]>;
			parent: string;
		}) => {
			if (isEmpty(glossaries.children)) return [];
			return glossaries.children
				.map((glossariesType) => {
					const getChild = () =>
						categoryRelation
							.map((obj) => {
								if (obj.parentCategoryGuid === glossariesType.categoryGuid) {
									return {
										name: obj.displayText,
										id: obj.displayText,
										children: [] as ChildrenInterface[],
										types: "child",
										parent: glossaries.parent,
										cGuid: glossaryType ? obj.termGuid : obj.categoryGuid,
										guid: glossary.guid
									};
								}
								return undefined;
							})
							.filter(Boolean) as ChildrenInterface[];
					if (glossariesType.parentCategoryGuid === undefined) {
						return {
							name: glossariesType.displayText,
							id: glossariesType.displayText,
							children: getChild(),
							types: "child",
							parent: glossaries.parent,
							cGuid: glossaryType
								? glossariesType.termGuid
								: glossariesType.categoryGuid,
							guid: glossary.guid
						} as ChildrenInterface;
					}
					return undefined;
				})
				.filter(Boolean) as ChildrenInterface[];
		};

		const children = getChildren({
			children: glossary?.terms ?? [],
			parent: glossary.name
		});

		return {
			[glossary.name]: {
				name: glossary.name,
				children: children || [],
				id: glossary.guid,
				types: "parent",
				parent: glossary.name,
				guid: glossary.guid
			}
		};
	});

	const child = (childs: ChildrenInterface[]): TreeNode[] => {
		if (!childs?.length) return [];
		return customSortBy(
			childs.map((obj) => ({
				id: obj?.name as string,
				label: obj?.name as string,
				children:
					obj?.children !== undefined
						? child(obj.children.filter(Boolean) as ChildrenInterface[])
						: [],
				types: obj?.types,
				parent: obj?.parent,
				guid: obj?.guid,
				cGuid: obj?.cGuid
			})),
			["label"]
		);
	};

	const sorted = customSortByObjectKeys(toServiceRows as ServiceTypeInterface[]);
	return sorted.map((entity: Record<string, {
		name: string;
		children: ChildrenInterface[];
		types: string;
		parent: string;
		guid: string;
	}>) => {
		const g = entity[Object.keys(entity)[0]];
		return {
			id: g.name,
			label: g.name,
			children: child((g.children ?? []) as ChildrenInterface[]),
			types: g.types,
			parent: g.parent,
			guid: g.guid
		};
	});
};

const collectGlossaryTermOptions = (
	nodes: TreeNode[],
	glossaryLabel: string,
	out: ScopedQuickSearchOption[]
) => {
	for (const n of nodes) {
		if (n.cGuid && n.types === "child" && n.id) {
			out.push({
				id: `term:${n.cGuid}`,
				title: `${n.label} (${glossaryLabel})`,
				group: "Glossary / Terms",
				kind: "glossary-term",
				termGuid: n.cGuid as string,
				glossaryGuid: (n.guid as string) ?? "",
				termId: n.id,
				termParent: (n.parent as string) ?? glossaryLabel
			});
		}
		if (n.children?.length) {
			collectGlossaryTermOptions(n.children, glossaryLabel, out);
		}
	}
};

export const glossaryTreeToScopedOptions = (
	tree: TreeNode[],
	searchTerm: string
): ScopedQuickSearchOption[] => {
	const filtered = filterTreeNodes(tree, searchTerm);
	const out: ScopedQuickSearchOption[] = [];
	for (const parent of filtered) {
		collectGlossaryTermOptions(parent.children ?? [], parent.label, out);
	}
	return customSortBy(out, ["title"]);
};

export const buildBusinessMetadataScopedOptions = (
	businessMetadataDefs: Array<{ name: string; guid: string }> | undefined,
	searchTerm: string
): ScopedQuickSearchOption[] => {
	if (!businessMetadataDefs?.length) return [];
	const q = searchTerm.trim().toLowerCase();
	return customSortBy(
		businessMetadataDefs
			.filter((d) => !q || d.name.toLowerCase().includes(q))
			.map((d) => ({
				id: `bm:${d.guid}`,
				title: d.name,
				group: "Business Metadata",
				kind: "business-metadata" as const,
				bmGuid: d.guid
			})),
		["title"]
	);
};
