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
	getEntityRefStatus,
	mergeReferredEntity
} from '../entityRefUtils';

describe('entityRefUtils', () => {
	describe('mergeReferredEntity', () => {
		it('merges referred entity data when guid exists in map', () => {
			const obj = { guid: 'g1', typeName: 'test_type' };
			const referredEntities = {
				g1: { name: 'Merged', status: 'DELETED' }
			};

			expect(mergeReferredEntity(obj, referredEntities)).toEqual({
				guid: 'g1',
				typeName: 'test_type',
				name: 'Merged',
				status: 'DELETED'
			});
		});

		it('returns original object when referredEntities is missing', () => {
			const obj = { guid: 'g1', name: 'Original' };

			expect(mergeReferredEntity(obj)).toEqual(obj);
		});

		it('returns original object when guid is not in referredEntities', () => {
			const obj = { guid: 'g1', name: 'Original' };

			expect(mergeReferredEntity(obj, { g2: { name: 'Other' } })).toEqual(obj);
		});
	});

	describe('getEntityRefStatus', () => {
		it('returns status when present', () => {
			expect(getEntityRefStatus({ status: 'ACTIVE' })).toBe('ACTIVE');
		});

		it('returns entityStatus when status is missing', () => {
			expect(getEntityRefStatus({ entityStatus: 'DELETED' })).toBe('DELETED');
		});

		it('returns id.state when status and entityStatus are missing', () => {
			expect(getEntityRefStatus({ id: { state: 'DELETED' } })).toBe('DELETED');
		});

		it('returns state when id is not an object', () => {
			expect(getEntityRefStatus({ state: 'ACTIVE' })).toBe('ACTIVE');
		});

		it('returns undefined when no status fields exist', () => {
			expect(getEntityRefStatus({ guid: 'g1' })).toBeUndefined();
		});
	});
});
