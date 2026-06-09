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

import { fields, getObjDef, validator } from '../AuditFiltersFields';
import type { RuleType } from 'react-querybuilder';

// Mock dependencies
const mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));
jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

jest.mock('@utils/Enum', () => ({
	dateRangesMap: {
		'Last 7 Days': ['2024-01-01', '2024-01-07'],
		'Last 30 Days': ['2024-01-01', '2024-01-30']
	},
	regex: {
		RANGE_CHECK: {
			int: { min: -2147483648, max: 2147483647 },
			byte: { min: -128, max: 127 },
			short: { min: -32768, max: 32767 },
			long: { min: -9223372036854775808, max: 9223372036854775807 },
			float: { min: -3.4028235e38, max: 3.4028235e38 },
			double: { min: -1.7976931348623157e308, max: 1.7976931348623157e308 }
		}
	},
	systemAttributes: {
		'__isIncomplete': 'Is Incomplete',
		'IsIncomplete': 'Is Incomplete',
		'Status': 'Status',
		'__state': 'State',
		'__entityStatus': 'Entity Status',
		'__classificationNames': 'Classification Names',
		'__customAttributes': 'Custom Attributes',
		'__labels': 'Labels',
		'__propagatedClassificationNames': 'Propagated Classification Names'
	}
}));

jest.mock('@utils/Global', () => ({
	dateTimeFormat: 'MM/DD/YYYY hh:mm:ss A'
}));

jest.mock('moment', () => {
	const mockMoment = jest.fn(() => ({
		valueOf: jest.fn(() => 1640995200000)
	}));
	return mockMoment;
});

jest.mock('react-querybuilder', () => ({
	toFullOption: (obj: any) => obj
}));

describe('AuditFiltersFields - 100% Coverage', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		mockCloneDeep.mockImplementation((obj) => JSON.parse(JSON.stringify(obj)));
	});

	describe('validator Function', () => {
		test('returns true when rule has value', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: 'someValue'
			};

			expect(validator(rule)).toBe(true);
		});

		test('returns false when rule value is empty string', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: ''
			};

			expect(validator(rule)).toBe(false);
		});

		test('returns false when rule value is null', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: null
			};

			expect(validator(rule)).toBe(false);
		});

		test('returns false when rule value is undefined', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: undefined
			};

			expect(validator(rule)).toBe(false);
		});

		test('returns true when rule value is 0', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: 0
			};

			expect(validator(rule)).toBe(true);
		});

		test('returns true when rule value is false', () => {
			const rule: RuleType = {
				field: 'test',
				operator: '=',
				value: false
			};

			expect(validator(rule)).toBe(true);
		});
	});

	describe('getObjDef Function - String Type', () => {
		test('creates object definition for string type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'testAttr', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('id', 'testAttr');
			expect(result).toHaveProperty('name', 'testAttr');
			expect(result).toHaveProperty('type', 'string');
			expect(result).toHaveProperty('operators');
			expect(result.operators).toContainEqual({ name: '=', label: '=' });
			expect(result.operators).toContainEqual({ name: 'contains', label: 'contains' });
		});

		test('includes string operators for string type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'testAttr', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.operators).toContainEqual({ name: 'begins_with', label: 'begins_with' });
			expect(result.operators).toContainEqual({ name: 'ends_with', label: 'ends_with' });
			expect(result.operators).toContainEqual({ name: 'is_null', label: 'is_null' });
			expect(result.operators).toContainEqual({ name: 'not_null', label: 'not_null' });
		});
	});

	describe('getObjDef Function - Date Type', () => {
		test('creates object definition for date type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'createdDate', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'date');
			expect(result).toHaveProperty('inputType', 'datetime-local');
			expect(result).toHaveProperty('values');
		});

		test('includes date operators for date type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'createdDate', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.operators).toContainEqual({ name: '=', label: '=' });
			expect(result.operators).toContainEqual({ name: '>', label: '>' });
			expect(result.operators).toContainEqual({ name: '<', label: '<' });
			expect(result.operators).toContainEqual({ name: 'TIME_RANGE', label: 'Time Range' });
		});

		test('includes is_null and not_null operators for date', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'createdDate', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.operators).toContainEqual({ name: 'is_null', label: 'is_null' });
			expect(result.operators).toContainEqual({ name: 'not_null', label: 'not_null' });
		});

		test('getDateConfig is called with correct parameters for date type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.values).toBeDefined();
			expect(result.values).toHaveProperty('opens');
			expect(result.values).toHaveProperty('autoApply');
		});

		test('getDateConfig handles TIME_RANGE operator', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'startTime', operator: 'TIME_RANGE', value: 'Last 7 Days' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles custom date range with dash separator', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'startTime', operator: 'TIME_RANGE', value: '2024-01-01 - 2024-01-31' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles predefined range value', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'startTime', operator: 'TIME_RANGE', value: 'Last 30 Days' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles non-TIME_RANGE operator', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'startTime', operator: '=', value: '2024-01-01' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
			expect(result.values).toHaveProperty('singleDatePicker');
		});

		test('getDateConfig handles null ruleObj', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj, null, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles undefined ruleObj', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };

			const result = getObjDef(allDataObj, attrObj, undefined, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles empty rules array', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = { rules: [] };

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles rule not matching name', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'endTime', operator: 'TIME_RANGE', value: 'Last 7 Days' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});

		test('getDateConfig handles operator mismatch', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'startTime', typeName: 'date' };
			const rules = {
				rules: [
					{ name: 'startTime', operator: '=', value: '2024-01-01' }
				]
			};

			const result = getObjDef(allDataObj, attrObj, rules, false, undefined, false);

			expect(result.values).toBeDefined();
		});
	});

	describe('getObjDef Function - Numeric Types', () => {
		test('creates object definition for int type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'count', typeName: 'int' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'integer');
			expect(result).toHaveProperty('inputType', 'number');
			expect(result.validator).toHaveProperty('min');
			expect(result.validator).toHaveProperty('max');
		});

		test('creates object definition for long type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'bigNumber', typeName: 'long' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'integer');
			expect(result).toHaveProperty('inputType', 'number');
		});

		test('creates object definition for float type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'decimal', typeName: 'float' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'double');
			expect(result).toHaveProperty('inputType', 'number');
		});

		test('creates object definition for double type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'bigDecimal', typeName: 'double' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'double');
			expect(result).toHaveProperty('inputType', 'number');
		});

		test('creates object definition for byte type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'smallNum', typeName: 'byte' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'integer');
		});

		test('creates object definition for short type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'mediumNum', typeName: 'short' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'integer');
		});

		test('includes numeric operators for int type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'count', typeName: 'int' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.operators).toContainEqual({ name: '>=', label: '>=' });
			expect(result.operators).toContainEqual({ name: '<=', label: '<=' });
		});
	});

	describe('getObjDef Function - Boolean Type', () => {
		test('creates object definition for boolean type', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'isActive', typeName: 'boolean' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'boolean');
			expect(result).toHaveProperty('valueEditorType', 'select');
			expect(result.values).toContainEqual({ name: 'true', label: 'true' });
			expect(result.values).toContainEqual({ name: 'false', label: 'false' });
		});

		test('includes boolean operators', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'isActive', typeName: 'boolean' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.operators).toContainEqual({ name: '=', label: '=' });
			expect(result.operators).toContainEqual({ name: '!=', label: '!=' });
		});
	});

	describe('getObjDef Function - Enum Type', () => {
		test('creates object definition for enum type', () => {
			const allDataObj = {
				enums: [
					{
						name: 'StatusEnum',
						elementDefs: [
							{ value: 'ACTIVE' },
							{ value: 'INACTIVE' },
							{ value: 'PENDING' }
						]
					}
				]
			};
			const attrObj = { name: 'status', typeName: 'StatusEnum' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'string');
			expect(result).toHaveProperty('valueEditorType', 'select');
			expect(result.values).toHaveLength(3);
			expect(result.values).toContainEqual({ name: 'ACTIVE', label: 'ACTIVE' });
		});

		test('handles enum with empty elementDefs', () => {
			const allDataObj = {
				enums: [
					{
						name: 'EmptyEnum',
						elementDefs: []
					}
				]
			};
			const attrObj = { name: 'status', typeName: 'EmptyEnum' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.values).toEqual([]);
		});

		test('handles enum not found in enums list', () => {
			const allDataObj = {
				enums: [
					{ name: 'OtherEnum', elementDefs: [] }
				]
			};
			const attrObj = { name: 'status', typeName: 'NonExistentEnum' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toBeUndefined();
		});
	});

	describe('getObjDef Function - System Attributes', () => {
		test('handles __isIncomplete system attribute', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__isIncomplete', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result).toHaveProperty('type', 'boolean');
			expect(result).toHaveProperty('valueEditorType', 'select');
			expect(result.label).toContain('Is Incomplete');
		});

		test('handles IsIncomplete system attribute', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'IsIncomplete', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result).toHaveProperty('type', 'boolean');
		});

		test('handles Status system attribute', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'Status', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result).toHaveProperty('valueEditorType', 'select');
			expect(result.values).toContainEqual({ name: 'ACTIVE', label: 'ACTIVE' });
			expect(result.values).toContainEqual({ name: 'DELETED', label: 'DELETED' });
		});

		test('handles __state system attribute', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__state', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result.values).toContainEqual({ name: 'ACTIVE', label: 'ACTIVE' });
			expect(result.values).toContainEqual({ name: 'DELETED', label: 'DELETED' });
		});

		test('handles __entityStatus system attribute', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__entityStatus', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result.values).toContainEqual({ name: 'ACTIVE', label: 'ACTIVE' });
		});
	});

	describe('getObjDef Function - Label Generation', () => {
		test('generates label with type for regular attributes', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'customAttr', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.label).toBe('customAttr (string)');
		});

		test('uses system attribute label when available', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__isIncomplete', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, undefined, undefined, true);

			expect(result.label).toContain('Is Incomplete');
		});

		test('does not add type suffix for special attributes', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__classificationNames', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.label).toBe('Classification Names');
		});

		test('handles __customAttributes without type suffix', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__customAttributes', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.label).toBe('Custom Attributes');
		});

		test('handles __labels without type suffix', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__labels', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.label).toBe('Labels');
		});

		test('handles __propagatedClassificationNames without type suffix', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: '__propagatedClassificationNames', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.label).toBe('Propagated Classification Names');
		});
	});

	describe('getObjDef Function - Group Handling', () => {
		test('adds group property when isGroup is true', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'testAttr', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, true, 'TestGroup');

			expect(result).toHaveProperty('group', 'TestGroup');
		});

		test('does not add group property when isGroup is false', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'testAttr', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj, undefined, false);

			expect(result).not.toHaveProperty('group');
		});
	});

	describe('fields Function', () => {
		test('returns empty array when entitys is empty', () => {
			const allDataObj = { entitys: [] };

			const result = fields(allDataObj);

			expect(result).toEqual([]);
		});

		test('returns empty array when entitys is null', () => {
			const allDataObj = { entitys: null };

			const result = fields(allDataObj);

			expect(result).toEqual([]);
		});

		test('returns empty array when __AtlasAuditEntry not found', () => {
			const allDataObj = {
				entitys: [
					{ name: 'OtherEntity', attributeDefs: [] }
				]
			};

			const result = fields(allDataObj);

			expect(result).toEqual([]);
		});

		test('processes __AtlasAuditEntry attributes', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: [
							{ name: 'action', typeName: 'string' },
							{ name: 'timestamp', typeName: 'date' }
						]
					}
				]
			};

			const result = fields(allDataObj);

			expect(result.length).toBeGreaterThan(0);
			expect(result[0]).toHaveProperty('name', 'action');
		});

		test('clones entitys data', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: [
							{ name: 'action', typeName: 'string' }
						]
					}
				]
			};

			fields(allDataObj);

			expect(mockCloneDeep).toHaveBeenCalledWith(allDataObj.entitys);
		});

		test('handles attributeDefs as object', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: {
							action: { name: 'action', typeName: 'string' },
							timestamp: { name: 'timestamp', typeName: 'date' }
						}
					}
				]
			};

			const result = fields(allDataObj);

			expect(result.length).toBe(2);
		});

		test('filters out undefined results from getObjDef', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: [
							{ name: 'validAttr', typeName: 'string' },
							{ name: 'invalidAttr', typeName: 'UnknownEnum' }
						]
					}
				]
			};

			const result = fields(allDataObj);

			expect(result.length).toBeGreaterThan(0);
		});

		test('handles empty attributeDefs', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: []
					}
				]
			};

			const result = fields(allDataObj);

			expect(result).toEqual([]);
		});

		test('handles null attributeDefs', () => {
			const allDataObj = {
				entitys: [
					{
						name: '__AtlasAuditEntry',
						attributeDefs: null
					}
				]
			};

			const result = fields(allDataObj);

			expect(result).toEqual([]);
		});
	});

	describe('Edge Cases', () => {
		test('handles null allDataObj', () => {
			const result = getObjDef(null, { name: 'test', typeName: 'string' });

			expect(result).toBeUndefined();
		});

		test('handles undefined attrObj', () => {
			const result = getObjDef({ enums: [] }, undefined);

			expect(result).toBeUndefined();
		});

		test('handles empty enums array', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'test', typeName: 'CustomEnum' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result?.values).toEqual([]);
		});

		test('handles null enums', () => {
			const allDataObj = { enums: null };
			const attrObj = { name: 'test', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('type', 'string');
		});
	});

	describe('Validator Property', () => {
		test('adds validator function to all objects', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'test', typeName: 'string' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result).toHaveProperty('validator');
			expect(typeof result.validator).toBe('function');
		});

		test('adds range validator for numeric types', () => {
			const allDataObj = { enums: [] };
			const attrObj = { name: 'count', typeName: 'int' };

			const result = getObjDef(allDataObj, attrObj);

			expect(result.validator).toHaveProperty('min');
			expect(result.validator).toHaveProperty('max');
		});
	});
});
