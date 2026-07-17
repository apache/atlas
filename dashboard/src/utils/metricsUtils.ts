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

export const sumEntityMetrics = (obj: Record<string, number> | undefined): number =>
	Object.values(obj || {}).reduce((a, b) => a + (Number(b) || 0), 0);

export const getEntityStatusTotals = (entity: Record<string, unknown> | undefined) => ({
	active: sumEntityMetrics(entity?.entityActive as Record<string, number>),
	shell: sumEntityMetrics(entity?.entityShell as Record<string, number>),
	deleted: sumEntityMetrics(entity?.entityDeleted as Record<string, number>)
});

export interface EntityTypeDistributionItem {
	name: string;
	count: number;
	active: number;
	deleted: number;
	/** Entity typedef names rolled into this bar (e.g. service type buckets). */
	underlyingTypeNames?: string[];
}

export const getTaggedCount = (tag: Record<string, unknown> | undefined): number =>
	sumEntityMetrics(tag?.tagEntities as Record<string, number>);

/** tagEntities values summed — counts tag–entity associations, not unique entities */
export const getTagEntityAssociationTotal = (tag: Record<string, unknown> | undefined): number =>
	getTaggedCount(tag);

/**
 * Number of classification types that have at least one active entity (each key in
 * tagEntities with count > 0). Does not double-count the same entity across types.
 */
export const getClassificationTypesInUseCount = (tag: Record<string, unknown> | undefined): number => {
	const tagEntities = tag?.tagEntities as Record<string, number> | undefined;
	if (!tagEntities || typeof tagEntities !== "object") return 0;
	return Object.values(tagEntities).filter((c) => (Number(c) || 0) > 0).length;
};

/** Classification names that have at least one entity assignment per tag metrics. */
export const getClassificationNamesInUseFromTag = (
	tag: Record<string, unknown> | undefined
): Set<string> => {
	const tagEntities = tag?.tagEntities as Record<string, number> | undefined;
	const names = new Set<string>();
	if (!tagEntities || typeof tagEntities !== "object") return names;
	for (const [name, count] of Object.entries(tagEntities)) {
		if ((Number(count) || 0) > 0) {
			names.add(name);
		}
	}
	return names;
};

/**
 * Defined classification names (from type defs) with no entity assignments in metrics.
 */
export const getUnusedClassificationNames = (
	definedNames: string[],
	tag: Record<string, unknown> | undefined
): string[] => {
	const inUse = getClassificationNamesInUseFromTag(tag);
	return definedNames.filter((n) => !inUse.has(n)).sort((a, b) => a.localeCompare(b));
};

/** Parse stats from metrics general.stats (keys like "Notification:currentDay" -> { Notification: { currentDay } }) */
export const parseMetricsStats = (stats: Record<string, unknown> | undefined): Record<string, Record<string, unknown>> => {
	const result: Record<string, Record<string, unknown>> = {};
	if (!stats || typeof stats !== "object") return result;
	for (const key of Object.keys(stats)) {
		const parts = key.split(":");
		const group = parts[0];
		const subKey = parts[1];
		if (!group || !subKey) continue;
		if (!result[group]) result[group] = {};
		result[group][subKey] = stats[key];
	}
	return result;
};

export interface MessageConsumptionItem {
	period: string;
	count: number;
	creates: number;
	updates: number;
	deletes: number;
	failed: number;
	avgTime: number;
}

const getNotificationValue = (
	notification: Record<string, unknown>,
	key: string,
	field: "creates" | "updates" | "deletes" | "failed" | "avgTime"
): number => {
	const keyMap: Record<string, Record<string, string>> = {
		total: { creates: "totalCreates", updates: "totalUpdates", deletes: "totalDeletes", failed: "totalFailed", avgTime: "totalAvgTime" },
		currentHour: { creates: "currentHourEntityCreates", updates: "currentHourEntityUpdates", deletes: "currentHourEntityDeletes", failed: "currentHourFailed", avgTime: "currentHourAvgTime" },
		previousHour: { creates: "previousHourEntityCreates", updates: "previousHourEntityUpdates", deletes: "previousHourEntityDeletes", failed: "previousHourFailed", avgTime: "previousHourAvgTime" },
		currentDay: { creates: "currentDayEntityCreates", updates: "currentDayEntityUpdates", deletes: "currentDayEntityDeletes", failed: "currentDayFailed", avgTime: "currentDayAvgTime" },
		previousDay: { creates: "previousDayEntityCreates", updates: "previousDayEntityUpdates", deletes: "previousDayEntityDeletes", failed: "previousDayFailed", avgTime: "previousDayAvgTime" }
	};
	const fullKey = keyMap[key]?.[field] ?? "";
	return Number(notification[fullKey] ?? 0);
};

export interface GetMessageConsumptionDataOptions {
	/** When false, omits the Total period (e.g. topic rows where total is shown as Processed). */
	includeTotal?: boolean;
}

/** Extract message consumption data for bar chart from Notification stats */
export const getMessageConsumptionData = (
	notification: Record<string, unknown> | undefined,
	options?: GetMessageConsumptionDataOptions
): MessageConsumptionItem[] => {
	if (!notification) return [];
	const includeTotal = options?.includeTotal !== false;
	const periods = [
		{ key: "total", label: "Total" },
		{ key: "currentHour", label: "Current Hour" },
		{ key: "previousHour", label: "Previous Hour" },
		{ key: "currentDay", label: "Current Day" },
		{ key: "previousDay", label: "Previous Day" }
	];
	const selected = includeTotal ? periods : periods.filter((p) => p.key !== "total");
	return selected.map(({ key, label }) => ({
		period: label,
		count: Number(notification[key] ?? 0),
		creates: getNotificationValue(notification, key, "creates"),
		updates: getNotificationValue(notification, key, "updates"),
		deletes: getNotificationValue(notification, key, "deletes"),
		failed: getNotificationValue(notification, key, "failed"),
		avgTime: getNotificationValue(notification, key, "avgTime")
	}));
};

/**
 * Topic partition maps sometimes repeat `Notification:*` keys from the wire format.
 * Strip the prefix so getMessageConsumptionData finds `currentHour`, `totalCreates`, etc.
 */
export const normalizeTopicMetricsRecord = (
	topicDetail: Record<string, unknown> | undefined
): Record<string, unknown> => {
	if (!topicDetail || typeof topicDetail !== "object") return {};
	const out: Record<string, unknown> = { ...topicDetail };
	for (const key of Object.keys(topicDetail)) {
		if (!key.startsWith("Notification:")) continue;
		const shortKey = key.slice("Notification:".length);
		if (shortKey && out[shortKey] === undefined) {
			out[shortKey] = topicDetail[key];
		}
	}
	return out;
};

/**
 * True when the payload includes at least one period bucket count (hour/day series).
 * `total` alone is not enough — legacy rows often only mirror processedMessageCount.
 */
export const topicRowHasPeriodMetrics = (topicDetail: Record<string, unknown> | undefined): boolean => {
	const n = normalizeTopicMetricsRecord(topicDetail);
	return (
		n.currentHour !== undefined ||
		n.previousHour !== undefined ||
		n.currentDay !== undefined ||
		n.previousDay !== undefined
	);
};

const omitTopicDetailsFromNotification = (
	notification: Record<string, unknown> | undefined
): Record<string, unknown> => {
	if (!notification || typeof notification !== "object") return {};
	const { topicDetails: _omit, ...rest } = notification;
	return rest;
};

/**
 * One entry from `Notification.topicDetails` for getMessageConsumptionData.
 * When the server omits per-period fields on partitions (legacy payload), merge in
 * aggregate `Notification` (excluding `topicDetails`) so the chart matches /admin/metrics.
 */
export const buildTopicNotificationRecord = (
	topicDetail: Record<string, unknown> | undefined,
	options?: {
		aggregateNotification?: Record<string, unknown> | undefined;
		/** When true, fill period keys from aggregate if the topic row lacks them. */
		useAggregateFallback?: boolean;
	}
): Record<string, unknown> | undefined => {
	if (!topicDetail || typeof topicDetail !== "object") {
		if (options?.useAggregateFallback && options?.aggregateNotification) {
			return omitTopicDetailsFromNotification(options.aggregateNotification);
		}
		return undefined;
	}
	const normalized = normalizeTopicMetricsRecord(topicDetail);
	if (!options?.useAggregateFallback || !options?.aggregateNotification) {
		return normalized;
	}
	const base = omitTopicDetailsFromNotification(options.aggregateNotification);
	return { ...base, ...normalized };
};

/** Chart periods without Total (Processed column shows the total). */
export const getMessageConsumptionDataExcludingTotal = (
	notification: Record<string, unknown> | undefined
): MessageConsumptionItem[] =>
	getMessageConsumptionData(notification, { includeTotal: false });

export interface ClassificationDistributionItem {
	name: string;
	count: number;
}

/** Top classifications by entity count from tag.tagEntities */
export const getClassificationDistribution = (
	tag: Record<string, unknown> | undefined,
	topN = 5
): ClassificationDistributionItem[] => {
	const tagEntities = tag?.tagEntities as Record<string, number> | undefined;
	if (!tagEntities) return [];
	return Object.entries(tagEntities)
		.map(([name, count]) => ({ name, count: Number(count) || 0 }))
		.sort((a, b) => b.count - a.count)
		.slice(0, topN);
};
