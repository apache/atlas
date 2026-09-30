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


/**
 * Unit tests for Global.ts
 * 
 * Coverage Target: 100% for Statements, Branches, Functions, and Lines
 */

import { globalSession, entityImgPath, dateTimeFormat, dateFormat } from '../Global';
import { globalSessionData } from '../Enum';

// Mock Enum module
jest.mock('../Enum', () => ({
	globalSessionData: {
		restCrsfHeader: '',
		crsfToken: '',
		debugMetrics: false,
		entityCreate: true,
		entityUpdate: true,
		taskTabEnabled: false,
		sessionTimeout: 900,
		uiTaskTabEnabled: false,
		relationshipSearch: false,
		isLineageOnDemandEnabled: false,
		lineageNodeCount: 3,
		isTimezoneFormatEnabled: true
	}
}));

describe('Global', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('exports', () => {
		it('should export entityImgPath constant', () => {
			expect(entityImgPath).toBe('/img/entity-icon/');
		});

		it('should export dateTimeFormat constant', () => {
			expect(dateTimeFormat).toBe('MM/DD/YYYY hh:mm:ss A');
		});

		it('should export dateFormat constant', () => {
			expect(dateFormat).toBe('MM/DD/YYYY');
		});

		it('should export globalSession function', () => {
			expect(typeof globalSession).toBe('function');
		});
	});

	describe('globalSession', () => {
		beforeEach(() => {
			jest.clearAllMocks();
			// Reset globalSessionData before each test to ensure test isolation
			Object.assign(globalSessionData, {
				restCrsfHeader: '',
				crsfToken: '',
				debugMetrics: false,
				entityCreate: true,
				entityUpdate: true,
				taskTabEnabled: false,
				sessionTimeout: 900,
				uiTaskTabEnabled: false,
				relationshipSearch: false,
				isLineageOnDemandEnabled: false,
				lineageNodeCount: 3,
				isTimezoneFormatEnabled: true
			});
		});

		it('should set restCrsfHeader from sessionData', () => {
			const sessionData = {
				'atlas.rest-csrf.custom-header': 'X-CSRF-TOKEN'
			};
			globalSession(sessionData);
			expect(globalSessionData.restCrsfHeader).toBe('X-CSRF-TOKEN');
		});

		it('should set restCrsfHeader to empty string when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.restCrsfHeader).toBe('');
		});

		it('should set crsfToken from sessionData', () => {
			const sessionData = {
				_csrfToken: 'test-token-123'
			};
			globalSession(sessionData);
			expect(globalSessionData.crsfToken).toBe('test-token-123');
		});

		it('should set debugMetrics from sessionData', () => {
			const sessionData = {
				'atlas.debug.metrics.enabled': true
			};
			globalSession(sessionData);
			expect(globalSessionData.debugMetrics).toBe(true);
		});

		it('should set entityCreate from sessionData', () => {
			const sessionData = {
				'atlas.entity.create.allowed': false
			};
			globalSession(sessionData);
			expect(globalSessionData.entityCreate).toBe(false);
		});

		it('should default entityCreate to true when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.entityCreate).toBe(true);
		});

		it('should set entityUpdate from sessionData', () => {
			const sessionData = {
				'atlas.entity.update.allowed': false
			};
			globalSession(sessionData);
			expect(globalSessionData.entityUpdate).toBe(false);
		});

		it('should default entityUpdate to true when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.entityUpdate).toBe(true);
		});

		it('should set taskTabEnabled from sessionData', () => {
			const sessionData = {
				'atlas.tasks.enabled': true
			};
			globalSession(sessionData);
			expect(globalSessionData.taskTabEnabled).toBe(true);
		});

		it('should default taskTabEnabled to false when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.taskTabEnabled).toBe(false);
		});

		it('should set sessionTimeout from sessionData', () => {
			const sessionData = {
				'atlas.session.timeout.secs': 1800
			};
			globalSession(sessionData);
			expect(globalSessionData.sessionTimeout).toBe(1800);
		});

		it('should default sessionTimeout to 900 when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.sessionTimeout).toBe(900);
		});

		it('should set uiTaskTabEnabled from sessionData', () => {
			const sessionData = {
				'atlas.tasks.ui.tab.enabled': true
			};
			globalSession(sessionData);
			expect(globalSessionData.uiTaskTabEnabled).toBe(true);
		});

		it('should set relationshipSearch from sessionData', () => {
			const sessionData = {
				'atlas.relationship.search.enabled': true
			};
			globalSession(sessionData);
			expect(globalSessionData.relationshipSearch).toBe(true);
		});

		it('should default relationshipSearch to false when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.relationshipSearch).toBe(false);
		});

		it('should set isLineageOnDemandEnabled from sessionData', () => {
			const sessionData = {
				'atlas.lineage.on.demand.enabled': true
			};
			globalSession(sessionData);
			expect(globalSessionData.isLineageOnDemandEnabled).toBe(true);
		});

		it('should default isLineageOnDemandEnabled to false when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.isLineageOnDemandEnabled).toBe(false);
		});

		it('should set lineageNodeCount from sessionData', () => {
			const sessionData = {
				'atlas.lineage.on.demand.default.node.count': 5
			};
			globalSession(sessionData);
			expect(globalSessionData.lineageNodeCount).toBe(5);
		});

		it('should default lineageNodeCount to 3 when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.lineageNodeCount).toBe(3);
		});

		it('should set isTimezoneFormatEnabled from sessionData', () => {
			const sessionData = {
				'atlas.ui.date.timezone.format.enabled': false
			};
			globalSession(sessionData);
			expect(globalSessionData.isTimezoneFormatEnabled).toBe(false);
		});

		it('should default isTimezoneFormatEnabled to true when not provided', () => {
			const sessionData = {};
			globalSession(sessionData);
			expect(globalSessionData.isTimezoneFormatEnabled).toBe(true);
		});

		it('should handle complete sessionData object', () => {
			const sessionData = {
				'atlas.rest-csrf.custom-header': 'X-CSRF-TOKEN',
				_csrfToken: 'token-123',
				'atlas.debug.metrics.enabled': true,
				'atlas.entity.create.allowed': false,
				'atlas.entity.update.allowed': false,
				'atlas.tasks.enabled': true,
				'atlas.session.timeout.secs': 1800,
				'atlas.tasks.ui.tab.enabled': true,
				'atlas.relationship.search.enabled': true,
				'atlas.lineage.on.demand.enabled': true,
				'atlas.lineage.on.demand.default.node.count': 5,
				'atlas.ui.date.timezone.format.enabled': false
			};
			globalSession(sessionData);
			expect(globalSessionData.restCrsfHeader).toBe('X-CSRF-TOKEN');
			expect(globalSessionData.crsfToken).toBe('token-123');
			expect(globalSessionData.debugMetrics).toBe(true);
			expect(globalSessionData.entityCreate).toBe(false);
			expect(globalSessionData.entityUpdate).toBe(false);
			expect(globalSessionData.taskTabEnabled).toBe(true);
			expect(globalSessionData.sessionTimeout).toBe(1800);
			expect(globalSessionData.uiTaskTabEnabled).toBe(true);
			expect(globalSessionData.relationshipSearch).toBe(true);
			expect(globalSessionData.isLineageOnDemandEnabled).toBe(true);
			expect(globalSessionData.lineageNodeCount).toBe(5);
			expect(globalSessionData.isTimezoneFormatEnabled).toBe(false);
		});
	});
});
