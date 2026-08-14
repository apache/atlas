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

/** Matches Entity Status Overview donut — single source for dashboard charts */
export const ENTITY_STATUS_DONUT_COLORS = {
	Active: "#10b981",
	Shell: "#f59e0b",
	Deleted: "#ef4444",
} as const;

/** Active primary series / bar fill (aligned with Classification Distribution bars) */
export const CHART_BAR_ACTIVE_BLUE = "#1976d2";

/** Horizontal bar charts: Y-axis title + ticks; keep left tight to reduce card gutter */
export const HORIZONTAL_BAR_CHART_MARGIN = {
	top: 8,
	right: 72,
	left: 44,
	bottom: 48,
} as const;

/** Classification bar chart: Y-axis width reserves label space; keep left margin minimal */
export const CLASSIFICATION_DISTRIBUTION_CHART_MARGIN = {
	top: 8,
	right: 72,
	left: 8,
	bottom: 48,
} as const;

export const CLASSIFICATION_Y_AXIS_MIN_WIDTH = 160;
export const CLASSIFICATION_Y_AXIS_MAX_WIDTH = 360;
export const CLASSIFICATION_Y_AXIS_CHAR_WIDTH = 8;
export const CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH = 30;
export const CLASSIFICATION_Y_AXIS_LABEL_SUFFIX = '...';

/** Truncate Y-axis classification labels for display (full name shown via SVG title on hover). */
export const truncateClassificationYAxisLabel = (label: string): string => {
	if (label.length <= CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH) {
		return label;
	}
	return `${label.slice(0, CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH)}${CLASSIFICATION_Y_AXIS_LABEL_SUFFIX}`;
};

export const isClassificationYAxisLabelTruncated = (label: string): boolean =>
	label.length > CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH;

/** Estimate Y-axis width from truncated label length (12px font, end-anchored ticks). */
export const getClassificationYAxisWidth = (labels: string[]): number => {
	if (!labels.length) return CLASSIFICATION_Y_AXIS_MIN_WIDTH;
	const longest = Math.max(
		...labels.map((label) => truncateClassificationYAxisLabel(label).length),
	);
	return Math.min(
		CLASSIFICATION_Y_AXIS_MAX_WIDTH,
		Math.max(CLASSIFICATION_Y_AXIS_MIN_WIDTH, longest * CLASSIFICATION_Y_AXIS_CHAR_WIDTH),
	);
};

/** Shared by classification and service type distribution chart Y-axis ticks */
export const truncateChartYAxisLabel = truncateClassificationYAxisLabel;
export const isChartYAxisLabelTruncated = isClassificationYAxisLabelTruncated;
export const getChartYAxisWidth = getClassificationYAxisWidth;
