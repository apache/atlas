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
	enrichEntityPayloadForRelationshipSave,
	isRelationshipAttrValueMissing,
	mergeMissingRelationshipAttributes
} from '../entityPayloadEnrichmentUtils';
import { getEntityWithRelationships } from '@api/apiMethods/detailpageApiMethod';

jest.mock('@api/apiMethods/detailpageApiMethod', () => ({
	getEntityWithRelationships: jest.fn()
}));

const mockGetEntityWithRelationships =
	getEntityWithRelationships as jest.MockedFunction<
		typeof getEntityWithRelationships
	>;

describe('entityPayloadEnrichmentUtils', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('isRelationshipAttrValueMissing', () => {
		it('returns true for undefined, null, empty array, object without guid', () => {
			expect(isRelationshipAttrValueMissing(undefined)).toBe(true);
			expect(isRelationshipAttrValueMissing(null)).toBe(true);
			expect(isRelationshipAttrValueMissing([])).toBe(true);
			expect(isRelationshipAttrValueMissing({})).toBe(true);
		});

		it('returns false for object with guid or non-empty primitive', () => {
			expect(
				isRelationshipAttrValueMissing({
					guid: 'g1',
					typeName: 'hive_table'
				})
			).toBe(false);
			expect(isRelationshipAttrValueMissing('value')).toBe(false);
		});
	});

	describe('mergeMissingRelationshipAttributes', () => {
		it('merges missing mandatory refs from full entity', () => {
			const entityJson = {
				guid: 'col-guid',
				typeName: 'hive_column',
				relationshipAttributes: {
					meanings: [{ guid: 'term-1' }]
				}
			};
			const fullRels = {
				table: { guid: 'table-guid', typeName: 'hive_table' },
				meanings: [{ guid: 'other-term' }]
			};

			mergeMissingRelationshipAttributes(entityJson, fullRels);

			expect(entityJson.relationshipAttributes.table).toEqual({
				guid: 'table-guid',
				typeName: 'hive_table'
			});
			expect(entityJson.relationshipAttributes.meanings).toEqual([
				{ guid: 'term-1' }
			]);
		});

		it('does not overwrite existing relationship values', () => {
			const entityJson = {
				relationshipAttributes: {
					table: { guid: 'existing', typeName: 'hive_table' }
				}
			};
			const fullRels = {
				table: { guid: 'new', typeName: 'hive_table' }
			};

			mergeMissingRelationshipAttributes(entityJson, fullRels);

			expect(entityJson.relationshipAttributes.table).toEqual({
				guid: 'existing',
				typeName: 'hive_table'
			});
		});
	});

	describe('enrichEntityPayloadForRelationshipSave', () => {
		it('fetches full entity and merges relationshipAttributes', async () => {
			const entityJson = {
				guid: 'col-guid',
				typeName: 'hive_column',
				relationshipAttributes: {}
			};
			mockGetEntityWithRelationships.mockResolvedValueOnce({
				data: {
					entity: {
						relationshipAttributes: {
							table: { guid: 'table-guid', typeName: 'hive_table' }
						}
					}
				}
			} as any);

			await enrichEntityPayloadForRelationshipSave(entityJson);

			expect(mockGetEntityWithRelationships).toHaveBeenCalledWith('col-guid');
			expect(entityJson.relationshipAttributes.table).toEqual({
				guid: 'table-guid',
				typeName: 'hive_table'
			});
		});

		it('skips fetch when guid is missing', async () => {
			await enrichEntityPayloadForRelationshipSave({ typeName: 'hive_column' });
			expect(mockGetEntityWithRelationships).not.toHaveBeenCalled();
		});

		it('continues when full entity GET fails', async () => {
			const entityJson = {
				guid: 'col-guid',
				relationshipAttributes: {}
			};
			mockGetEntityWithRelationships.mockRejectedValueOnce(
				new Error('network')
			);

			await expect(
				enrichEntityPayloadForRelationshipSave(entityJson)
			).resolves.toBeUndefined();
		});
	});
});
