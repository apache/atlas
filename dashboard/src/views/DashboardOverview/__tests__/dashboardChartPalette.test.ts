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

import {
	CLASSIFICATION_Y_AXIS_CHAR_WIDTH,
	CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH,
	CLASSIFICATION_Y_AXIS_LABEL_SUFFIX,
	CLASSIFICATION_Y_AXIS_MAX_WIDTH,
	CLASSIFICATION_Y_AXIS_MIN_WIDTH,
	getChartYAxisWidth,
	getClassificationYAxisWidth,
	isChartYAxisLabelTruncated,
	isClassificationYAxisLabelTruncated,
	truncateChartYAxisLabel,
	truncateClassificationYAxisLabel,
} from '../dashboardChartPalette';

describe('dashboardChartPalette', () => {
	describe('truncateClassificationYAxisLabel', () => {
		it('returns the label unchanged when within max length', () => {
			expect(truncateClassificationYAxisLabel('PII')).toBe('PII');
			expect(truncateClassificationYAxisLabel('a'.repeat(30))).toBe(
				'a'.repeat(30),
			);
		});

		it('truncates to 30 characters and appends ellipsis', () => {
			const fullName = 'a'.repeat(45);
			expect(truncateClassificationYAxisLabel(fullName)).toBe(
				`${'a'.repeat(CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH)}${CLASSIFICATION_Y_AXIS_LABEL_SUFFIX}`,
			);
		});
	});

	describe('isClassificationYAxisLabelTruncated', () => {
		it('returns false for labels at or below max length', () => {
			expect(isClassificationYAxisLabelTruncated('PII')).toBe(false);
			expect(isClassificationYAxisLabelTruncated('a'.repeat(30))).toBe(false);
		});

		it('returns true for labels longer than max length', () => {
			expect(isClassificationYAxisLabelTruncated('a'.repeat(31))).toBe(true);
		});
	});

	describe('getClassificationYAxisWidth', () => {
		it('returns minimum width when labels are empty', () => {
			expect(getClassificationYAxisWidth([])).toBe(
				CLASSIFICATION_Y_AXIS_MIN_WIDTH,
			);
		});

		it('returns minimum width for short classification names', () => {
			expect(getClassificationYAxisWidth(['PII', 'HIPAA'])).toBe(
				CLASSIFICATION_Y_AXIS_MIN_WIDTH,
			);
		});

		it('scales width from truncated display length', () => {
			const longLabel = 'a'.repeat(25);
			expect(getClassificationYAxisWidth(['PII', longLabel])).toBe(
				25 * CLASSIFICATION_Y_AXIS_CHAR_WIDTH,
			);
		});

		it('uses truncated length cap for very long classification names', () => {
			const veryLongLabel = 'a'.repeat(80);
			const truncatedLength =
				CLASSIFICATION_Y_AXIS_LABEL_MAX_LENGTH +
				CLASSIFICATION_Y_AXIS_LABEL_SUFFIX.length;
			expect(getClassificationYAxisWidth([veryLongLabel])).toBe(
				truncatedLength * CLASSIFICATION_Y_AXIS_CHAR_WIDTH,
			);
		});

		it('does not exceed maximum width', () => {
			const labels = Array.from({ length: 10 }, () => 'x'.repeat(80));
			expect(getClassificationYAxisWidth(labels)).toBeLessThanOrEqual(
				CLASSIFICATION_Y_AXIS_MAX_WIDTH,
			);
		});
	});

	describe('shared chart Y-axis aliases', () => {
		it('exposes the same helpers for service type and classification charts', () => {
			const label = 'a'.repeat(45);
			expect(truncateChartYAxisLabel(label)).toBe(
				truncateClassificationYAxisLabel(label),
			);
			expect(isChartYAxisLabelTruncated(label)).toBe(
				isClassificationYAxisLabelTruncated(label),
			);
			expect(getChartYAxisWidth([label])).toBe(
				getClassificationYAxisWidth([label]),
			);
		});
	});
});
