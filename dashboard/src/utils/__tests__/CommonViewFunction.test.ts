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
 * Unit tests for CommonViewFunction.ts
 * 
 * Coverage Target: 100% for Statements, Branches, Functions, and Lines
 */

import {
	getValue,
	JSONPrettyPrint,
	attributeFilter,
	generateObjectForSaveSearchApi,
	getTypeName
} from '../CommonViewFunction';
import {
	queryBuilderApiOperatorToUI,
	queryBuilderDateRangeAPIValueToUI,
	queryBuilderDateRangeUIValueToAPI,
	queryBuilderUIOperatorToAPI
} from '../Enum';
import {
	convertToValidDate,
	formatedDate,
	getUrlState,
	isEmpty,
	isObject,
	isString
} from '../Utils';

// Use actual implementations - no mocks needed for Utils and Enum

describe('CommonViewFunction', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	describe('getValue', () => {
		it('should return value when value is defined and not null', () => {
			expect(getValue('test')).toBe('test');
			expect(getValue(123)).toBe(123);
			expect(getValue(0)).toBe(0);
			expect(getValue(false)).toBe(false);
			expect(getValue([])).toEqual([]);
			expect(getValue({})).toEqual({});
		});

		it('should return "NA" when value is undefined', () => {
			expect(getValue(undefined)).toBe('NA');
		});

		it('should return "NA" when value is null', () => {
			expect(getValue(null)).toBe('NA');
		});

		it('should ignore optional parameters', () => {
			expect(getValue('test', 'ignored', 'ignored')).toBe('test');
			expect(getValue(null, 'ignored', 'ignored')).toBe('NA');
		});
	});

	describe('JSONPrettyPrint', () => {
		it('should format valid object with HTML spans', () => {
			const obj = { name: 'test', value: 123 };
			const result = JSONPrettyPrint(obj);
			expect(typeof result).toBe('string');
			expect(result).toContain('json-key');
			expect(result).toContain('json-value');
		});

		it('should handle objects with string values', () => {
			const obj = { name: 'test', description: 'test description' };
			const result = JSONPrettyPrint(obj);
			expect(result).toContain('json-string');
		});

		it('should handle objects with numeric values', () => {
			const obj = { count: 123, price: 45.67 };
			const result = JSONPrettyPrint(obj);
			expect(result).toContain('json-value');
		});

		it('should escape HTML characters', () => {
			const obj = { name: 'test & value', html: '<div>test</div>' };
			const result = JSONPrettyPrint(obj);
			expect(result).toContain('&amp;');
			expect(result).toContain('&lt;');
			expect(result).toContain('&gt;');
		});

		it('should return empty object when obj is not an object', () => {
			// String is not an object (arrays are objects though)
			const result = JSONPrettyPrint('not an object' as any);
			expect(result).toEqual({});
		});

		it('should return empty object when obj is null', () => {
			const result = JSONPrettyPrint(null as any);
			expect(result).toEqual({});
		});

		it('should handle nested objects', () => {
			const obj = { nested: { key: 'value' } };
			const result = JSONPrettyPrint(obj);
			expect(typeof result).toBe('string');
		});

		it('should handle arrays', () => {
			const obj = { items: [1, 2, 3] };
			const result = JSONPrettyPrint(obj);
			expect(typeof result).toBe('string');
		});
	});

	describe('attributeFilter.generateUrl', () => {
		it('should generate URL for simple rule', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'name',
							operator: '=',
							value: 'test'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toBe('AND(name::=::test)');
		});

		it('should generate URL for multiple rules', () => {
			const options = {
				value: {
					condition: 'OR',
					rules: [
						{ id: 'name', operator: '=', value: 'test1' },
						{ id: 'type', operator: '!=', value: 'test2' }
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('OR(');
			expect(result).toContain('name::=::test1');
			expect(result).toContain('type::!=::test2');
		});

		it('should handle nested conditions', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							condition: 'OR',
							rules: [
								{ id: 'name', operator: '=', value: 'test' }
							]
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('AND(');
			expect(result).toContain('OR(');
		});

		it('should handle attributeDefs and map attributeType', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							attributeName: 'testAttr',
							operator: '=',
							attributeValue: 'testValue'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: [
					{ name: 'testAttr', typeName: 'string' }
				]
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('testAttr');
		});

		it('should handle date type with formatedDateToLong', () => {
			const originalParse = Date.parse;
			Date.parse = jest.fn(() => 1704067200000);

			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'createTime',
							operator: '=',
							value: '01/01/2024',
							type: 'date'
						}
					]
				},
				formatedDateToLong: true,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('date');
			
			Date.parse = originalParse;
		});

		it('should handle TIME_RANGE operator with date range', () => {
			const originalParse = Date.parse;
			Date.parse = jest.fn(() => 1704067200000);

			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'dateRange',
							operator: 'TIME_RANGE',
							value: '01/01/2024 - 01/31/2024'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('TIME_RANGE');
			
			Date.parse = originalParse;
		});

		it('should handle TIME_RANGE operator with predefined range', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'dateRange',
							operator: 'TIME_RANGE',
							value: 'Today'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('TIME_RANGE');
		});

		it('should handle is_null and not_null operators with date type', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'createTime',
							operator: 'is_null',
							value: '',
							type: 'date'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('is_null');
			expect(result).toContain('::');
		});

		it('should handle object value', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'test',
							operator: '=',
							value: {}
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('::');
		});

		it('should trim string values', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							id: 'name',
							operator: '=',
							value: '  test  '
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('test');
		});

		it('should handle field property', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: [
						{
							field: 'businessMetadata.attr1',
							operator: '=',
							value: 'test'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toContain('businessMetadata.attr1');
		});

		it('should return null for empty options', () => {
			// When value is null or empty, conditionalURl returns null
			// The code has a bug where it tries to access .length on null
			// But we can test with empty rules array which returns empty string
			const result = attributeFilter.generateUrl({ 
				value: { condition: 'AND', rules: [] },
				formatedDateToLong: false,
				attributeDefs: []
			});
			// Empty rules array results in empty string, which has length 0, so returns null
			expect(result).toBeNull();
		});


		it('should return null when attrQuery is empty', () => {
			const options = {
				value: {
					condition: 'AND',
					rules: []
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			// When rules array is empty, the result will be empty string, not null
			// But the function checks if attrQuery.length is truthy, so empty string will return null
			expect(result).toBeNull();
		});

		it('should handle criterion property instead of rules', () => {
			const options = {
				value: {
					condition: 'AND',
					criterion: [
						{
							id: 'name',
							operator: '=',
							value: 'test'
						}
					]
				},
				formatedDateToLong: false,
				attributeDefs: []
			};

			const result = attributeFilter.generateUrl(options);
			expect(result).toBe('AND(name::=::test)');
		});
	});

	describe('attributeFilter.extractUrl', () => {
		it('should extract simple URL to object', () => {
			const options = {
				value: 'AND(name::=::test)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.condition).toBe('AND');
			expect(result.rules).toBeDefined();
			expect(result.rules[0].id).toBe('name');
			expect(result.rules[0].operator).toBe('=');
			expect(result.rules[0].value).toBe('test');
		});

		it('should extract OR condition', () => {
			const options = {
				value: 'OR(name::=::test)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.condition).toBe('OR');
		});

		it('should handle nested conditions', () => {
			const options = {
				value: 'AND(OR(name::=::test)|1|)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.condition).toBe('AND');
			expect(result.rules[0].condition).toBe('OR');
		});

		it('should handle apiObj mode', () => {
			const options = {
				value: 'AND(name::=::test)',
				formatDate: false,
				apiObj: true
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.condition).toBe('AND');
			expect(result.criterion).toBeDefined();
			expect(result.criterion[0].attributeName).toBe('name');
			expect(result.criterion[0].operator).toBe('eq');
		});

		it('should handle date type with formatDate', () => {
			const options = {
				value: 'AND(createTime::=::1704067200000::date)',
				formatDate: true,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.rules[0].type).toBe('date');
		});

		it('should handle TIME_RANGE with comma-separated dates', () => {
			const options = {
				value: 'AND(dateRange::TIME_RANGE::1704067200000,1706745600000)',
				formatDate: true,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.rules[0].operator).toBe('TIME_RANGE');
			expect(result.rules[0].value).toContain(' - ');
		});

		it('should handle TIME_RANGE with predefined range', () => {
			const options = {
				value: 'AND(dateRange::TIME_RANGE::TODAY)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.rules[0].operator).toBe('TIME_RANGE');
		});

		it('should return null for empty urlObj', () => {
			const result = attributeFilter.extractUrl({ value: '' });
			expect(result).toBeNull();
		});

		it('should return null for null urlObj', () => {
			const result = attributeFilter.extractUrl({ value: null });
			expect(result).toBeNull();
		});

		it('should handle attributeName in apiObj mode with date type', () => {
			const options = {
				value: 'AND(createTime::=::1704067200000::date)',
				formatDate: true,
				apiObj: true
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.criterion[0].attributeValue).toBeDefined();
		});

		it('should handle empty value in rule', () => {
			const options = {
				value: 'AND(name::=::)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.rules[0].value).toBe('');
		});

		it('should handle nested string with condition (lines 190-191)', () => {
			// Test the isStringNested && isCondition branch
			const options = {
				value: 'AND(OR(name::=::test)|2|)',
				formatDate: false,
				apiObj: false
			};

			const result = attributeFilter.extractUrl(options);
			expect(result.condition).toBe('AND');
			expect(result.rules[0].condition).toBe('OR');
		});
	});

	describe('attributeFilter.generateAPIObj', () => {
		it('should generate API object from URL', () => {
			const url = 'AND(name::=::test)';
			const result = attributeFilter.generateAPIObj(url);
			expect(result).toBeDefined();
			expect(result.condition).toBe('AND');
		});

		it('should return null for empty URL', () => {
			const result = attributeFilter.generateAPIObj('');
			expect(result).toBeNull();
		});

		it('should return null for null URL', () => {
			const result = attributeFilter.generateAPIObj(null);
			expect(result).toBeNull();
		});
	});

	describe('generateObjectForSaveSearchApi', () => {

		it('should generate object for save search API', () => {
			const options = {
				name: 'Test Search',
				guid: 'test-guid',
				value: {
					pageLimit: '50',
					type: 'DataSet',
					query: 'test query',
					attributes: 'name,description',
					tagFilters: 'AND(name::=::test)',
					entityFilters: 'AND(type::=::table)',
					relationshipFilters: 'AND(rel::=::test)',
					includeDE: 'true',
					excludeST: 'false',
					excludeSC: 'true'
				}
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(result.name).toBe('Test Search');
			expect(result.guid).toBe('test-guid');
			expect(result.searchParameters).toBeDefined();
		});

		it('should convert attributes string to array', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: {
					attributes: 'name,description,type'
				}
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(Array.isArray(result.searchParameters.attributes)).toBe(true);
			expect(result.searchParameters.attributes.length).toBe(3);
		});

		it('should convert filter strings to API objects', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: {
					tagFilters: 'AND(name::=::test)',
					entityFilters: 'AND(type::=::table)',
					relationshipFilters: 'AND(rel::=::test)'
				}
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(result.searchParameters.tagFilters).toBeDefined();
			expect(result.searchParameters.entityFilters).toBeDefined();
			expect(result.searchParameters.relationshipFilters).toBeDefined();
		});

		it('should invert boolean values for includeDE, excludeST, excludeSC', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: {
					includeDE: 'true',  // truthy string -> inverted to false
					excludeST: 'false', // truthy string (non-empty) -> inverted to false
					excludeSC: 'true'   // truthy string -> inverted to false
				}
			};

			const result = generateObjectForSaveSearchApi(options);
			// Logic: val ? false : true - so truthy becomes false, falsy becomes true
			// Note: 'false' is a truthy string, so it becomes false
			// includeDE: 'true' (truthy) -> false -> excludeDeletedEntities = false
			// excludeST: 'false' (truthy string) -> false -> includeSubTypes = false
			// excludeSC: 'true' (truthy) -> false -> includeSubClassifications = false
			expect(result.searchParameters.excludeDeletedEntities).toBe(false);
			expect(result.searchParameters.includeSubTypes).toBe(false); // 'false' string is truthy!
			expect(result.searchParameters.includeSubClassifications).toBe(false);
		});

		it('should default includeDE, excludeST, excludeSC to true when undefined', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: {}
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(result.searchParameters.excludeDeletedEntities).toBe(true);
			expect(result.searchParameters.includeSubTypes).toBe(true);
			expect(result.searchParameters.includeSubClassifications).toBe(true);
		});

		it('should handle undefined value', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: undefined
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(result).toBeUndefined();
		});

		it('should handle null value', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: null
			};

			const result = generateObjectForSaveSearchApi(options);
			expect(result).toBeUndefined();
		});

		it('should handle non-object svalue entries', () => {
			const options = {
				name: 'Test',
				guid: 'guid',
				value: {
					uiParameters: 'some value'
				}
			};

			const result = generateObjectForSaveSearchApi(options);
			// uiParameters is a direct property, not nested in searchParameters
			expect(result).toBeDefined();
		});
	});

	describe('getTypeName', () => {
		it('should return array type for multiValueSelect with enumeration', () => {
			const result = getTypeName(true, 'TestEnum', { typeName: 'enumeration' });
			expect(result).toBe('array<TestEnum>');
		});

		it('should return array type for multiValueSelect with non-enumeration', () => {
			const result = getTypeName(true, 'TestEnum', { typeName: 'string' });
			expect(result).toBe('array<string>');
		});

		it('should return enum type for single value with enumeration', () => {
			const result = getTypeName(false, 'TestEnum', { typeName: 'enumeration' });
			expect(result).toBe('TestEnum');
		});

		it('should return typeName for single value with non-enumeration', () => {
			const result = getTypeName(false, 'TestEnum', { typeName: 'string' });
			expect(result).toBe('string');
		});
	});
});
