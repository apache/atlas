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
 * Unit tests for Helper.ts
 * 
 * Coverage Target: 100% for Statements, Branches, Functions, and Lines
 */

// Mock Utils module BEFORE imports
// Note: isObject mock excludes arrays to match toArrayifObject behavior
const mockIsEmpty = jest.fn((val) => {
	if (val === null || val === undefined) return true;
	if (typeof val === 'object' && Object.keys(val).length === 0) return true;
	if (typeof val === 'string' && val.trim().length === 0) return true;
	return false;
});

const mockIsObject = jest.fn((val) => {
	// Exclude arrays to match expected toArrayifObject behavior
	return val !== null && typeof val === 'object' && !Array.isArray(val);
});

jest.mock('../Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	isObject: (...args: any[]) => mockIsObject(...args)
}));

import {
	startsWith,
	uniq,
	toArrayifObject,
	cloneDeep,
	isEmptyObject,
	extend,
	sortByKeyWithUnderscoreFirst,
	omit,
	invert,
	numberFormatWithComma,
	numberFormatWithBytes,
	without,
	union,
	customSortObj,
	isEmptyValueCheck
} from '../Helper';
import { isEmpty, isObject } from '../Utils';

describe('Helper', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		// Reset mock implementations
		mockIsEmpty.mockImplementation((val) => {
			if (val === null || val === undefined) return true;
			if (typeof val === 'object' && Object.keys(val).length === 0) return true;
			if (typeof val === 'string' && val.trim().length === 0) return true;
			return false;
		});
		mockIsObject.mockImplementation((val) => {
			// Exclude arrays to match expected toArrayifObject behavior
			return val !== null && typeof val === 'object' && !Array.isArray(val);
		});
	});

	describe('startsWith', () => {
		it('should return true when string starts with matchStr', () => {
			expect(startsWith('hello world', 'hello')).toBe(true);
			expect(startsWith('test', 'te')).toBe(true);
			expect(startsWith('abc', 'a')).toBe(true);
		});

		it('should return false when string does not start with matchStr', () => {
			expect(startsWith('hello world', 'world')).toBe(false);
			expect(startsWith('test', 'est')).toBe(false);
		});

		it('should return undefined when str is null', () => {
			expect(startsWith(null as any, 'test')).toBeUndefined();
		});

		it('should return undefined when str is undefined', () => {
			expect(startsWith(undefined as any, 'test')).toBeUndefined();
		});

		it('should return undefined when matchStr is null', () => {
			expect(startsWith('test', null as any)).toBeUndefined();
		});

		it('should return undefined when matchStr is undefined', () => {
			expect(startsWith('test', undefined as any)).toBeUndefined();
		});

		it('should return undefined when str is not a string', () => {
			expect(startsWith(123 as any, 'test')).toBeUndefined();
		});

		it('should return undefined when matchStr is not a string', () => {
			expect(startsWith('test', 123 as any)).toBeUndefined();
		});
	});

	describe('uniq', () => {
		it('should return unique values for unsorted array without iteratee', () => {
			const result = uniq([1, 2, 2, 3, 1, 4], false);
			expect(result).toEqual([1, 2, 3, 4]);
		});

		it('should return unique values for sorted array without iteratee', () => {
			const result = uniq([1, 1, 2, 2, 3, 4], true);
			expect(result).toEqual([1, 2, 3, 4]);
		});

		it('should return unique values using iteratee for unsorted array', () => {
			const result = uniq(
				[{ id: 1 }, { id: 2 }, { id: 1 }, { id: 3 }],
				false,
				(item) => item.id
			);
			expect(result.length).toBe(3);
			expect(result.map((r) => r.id)).toEqual([1, 2, 3]);
		});

		it('should return unique values using iteratee for sorted array', () => {
			const result = uniq(
				[{ id: 1 }, { id: 1 }, { id: 2 }, { id: 3 }],
				true,
				(item) => item.id
			);
			expect(result.length).toBe(3);
			expect(result.map((r) => r.id)).toEqual([1, 2, 3]);
		});

		it('should handle empty array', () => {
			expect(uniq([], false)).toEqual([]);
			expect(uniq([], true)).toEqual([]);
		});

		it('should handle array with single element', () => {
			expect(uniq([1], false)).toEqual([1]);
			expect(uniq([1], true)).toEqual([1]);
		});

		it('should handle array with all unique values', () => {
			expect(uniq([1, 2, 3, 4], false)).toEqual([1, 2, 3, 4]);
		});

		it('should handle array with all duplicate values', () => {
			expect(uniq([1, 1, 1, 1], false)).toEqual([1]);
		});
	});

	describe('toArrayifObject', () => {
		it('should convert object to array', () => {
			const obj = { name: 'test' };
			const result = toArrayifObject(obj);
			expect(Array.isArray(result)).toBe(true);
			expect(result).toEqual([obj]);
			// Verify mock was called
			expect(mockIsObject).toHaveBeenCalledWith(obj);
		});

		it('should return array as is', () => {
			const arr = [1, 2, 3];
			const result = toArrayifObject(arr);
			expect(result).toBe(arr);
		});

		it('should return non-object values as is (line 76 else branch)', () => {
			expect(toArrayifObject('test')).toBe('test');
			expect(toArrayifObject(123)).toBe(123);
			expect(toArrayifObject(null)).toBe(null);
			expect(toArrayifObject(undefined)).toBe(undefined);
			expect(toArrayifObject(true)).toBe(true);
			expect(toArrayifObject(false)).toBe(false);
		});
	});

	describe('cloneDeep', () => {
		it('should return null for null input', () => {
			expect(cloneDeep(null)).toBeNull();
		});

		it('should return string as is', () => {
			expect(cloneDeep('test')).toBe('test');
		});

		it('should return number as is', () => {
			expect(cloneDeep(123)).toBe(123);
		});

		it('should return boolean as is', () => {
			expect(cloneDeep(true)).toBe(true);
			expect(cloneDeep(false)).toBe(false);
		});

		it('should clone Date object', () => {
			const date = new Date('2024-01-01');
			const cloned = cloneDeep(date);
			expect(cloned).toBeInstanceOf(Date);
			expect(cloned.getTime()).toBe(date.getTime());
			expect(cloned).not.toBe(date);
		});

		it('should deep clone array', () => {
			const arr = [1, 2, { nested: 'value' }];
			const cloned = cloneDeep(arr);
			expect(cloned).toEqual(arr);
			expect(cloned).not.toBe(arr);
			expect(cloned[2]).not.toBe(arr[2]);
		});

		it('should deep clone object', () => {
			const obj = { a: 1, b: { nested: 'value' } };
			const cloned = cloneDeep(obj);
			expect(cloned).toEqual(obj);
			expect(cloned).not.toBe(obj);
			expect(cloned.b).not.toBe(obj.b);
		});

		it('should handle nested arrays and objects', () => {
			const complex = {
				arr: [1, { nested: 'value' }],
				obj: { nested: { deep: 'value' } }
			};
			const cloned = cloneDeep(complex);
			expect(cloned).toEqual(complex);
			expect(cloned.arr).not.toBe(complex.arr);
			expect(cloned.obj).not.toBe(complex.obj);
			expect(cloned.arr[1]).not.toBe(complex.arr[1]);
			expect(cloned.obj.nested).not.toBe(complex.obj.nested);
		});

		it('should handle empty object', () => {
			const obj = {};
			const cloned = cloneDeep(obj);
			expect(cloned).toEqual({});
			expect(cloned).not.toBe(obj);
		});

		it('should handle empty array', () => {
			const arr: any[] = [];
			const cloned = cloneDeep(arr);
			expect(cloned).toEqual([]);
			expect(cloned).not.toBe(arr);
		});

		it('should return param as-is for unsupported types (line 108)', () => {
			// Test fallback case when param doesn't match any known type
			// This covers line 108 which returns param for undefined/unsupported types
			const symbol = Symbol('test');
			const result = cloneDeep(symbol);
			expect(result).toBe(symbol);
		});
	});

	describe('isEmptyObject', () => {
		it('should return true for empty object', () => {
			expect(isEmptyObject({})).toBe(true);
		});

		it('should return true for object with empty string key', () => {
			expect(isEmptyObject({ '': '' })).toBe(true);
		});

		it('should return false for object with properties', () => {
			expect(isEmptyObject({ name: 'test' })).toBe(false);
		});

		it('should return false for object with multiple properties', () => {
			expect(isEmptyObject({ a: 1, b: 2 })).toBe(false);
		});

		it('should return false for object with empty string key and other properties', () => {
			expect(isEmptyObject({ '': '', name: 'test' })).toBe(false);
		});
	});

	describe('extend', () => {
		it('should shallow merge objects', () => {
			const result = extend({ a: 1 }, { b: 2 });
			expect(result).toEqual({ a: 1, b: 2 });
		});

		it('should merge multiple objects', () => {
			const result = extend({ a: 1 }, { b: 2 }, { c: 3 });
			expect(result).toEqual({ a: 1, b: 2, c: 3 });
		});

		it('should override properties from later objects', () => {
			const result = extend({ a: 1 }, { a: 2, b: 3 });
			expect(result).toEqual({ a: 2, b: 3 });
		});

		it('should perform deep merge when first argument is true', () => {
			const result = extend(true, { a: { b: 1 } }, { a: { c: 2 } });
			expect(result.a).toEqual({ b: 1, c: 2 });
		});

		it('should shallow merge when deep is false', () => {
			const result = extend(false, { a: { b: 1 } }, { a: { c: 2 } });
			expect(result.a).toEqual({ c: 2 });
		});

		it('should handle empty objects', () => {
			expect(extend({}, {})).toEqual({});
		});

		it('should handle single object', () => {
			expect(extend({ a: 1 })).toEqual({ a: 1 });
		});

		it('should handle no arguments', () => {
			expect(extend()).toEqual({});
		});

		it('should only copy own properties', () => {
			const obj1 = Object.create({ inherited: 'value' });
			obj1.own = 'value';
			const result = extend({}, obj1);
			expect(result.own).toBe('value');
			expect(result.inherited).toBeUndefined();
		});
	});

	describe('sortByKeyWithUnderscoreFirst', () => {
		it('should sort objects with underscore-prefixed keys first', () => {
			const arr = [
				{ name: 'zebra' },
				{ name: '_alpha' },
				{ name: 'beta' },
				{ name: '_gamma' }
			];
			const result = sortByKeyWithUnderscoreFirst(arr, 'name');
			expect(result[0].name).toBe('_alpha');
			expect(result[1].name).toBe('_gamma');
			expect(result[2].name).toBe('beta');
			expect(result[3].name).toBe('zebra');
		});

		it('should sort underscore-prefixed keys alphabetically', () => {
			const arr = [{ name: '_zebra' }, { name: '_alpha' }, { name: '_beta' }];
			const result = sortByKeyWithUnderscoreFirst(arr, 'name');
			expect(result[0].name).toBe('_alpha');
			expect(result[1].name).toBe('_beta');
			expect(result[2].name).toBe('_zebra');
		});

		it('should sort non-underscore keys alphabetically', () => {
			const arr = [{ name: 'zebra' }, { name: 'alpha' }, { name: 'beta' }];
			const result = sortByKeyWithUnderscoreFirst(arr, 'name');
			expect(result[0].name).toBe('alpha');
			expect(result[1].name).toBe('beta');
			expect(result[2].name).toBe('zebra');
		});

		it('should handle empty array', () => {
			expect(sortByKeyWithUnderscoreFirst([], 'name')).toEqual([]);
		});

		it('should handle array with single element', () => {
			const arr = [{ name: 'test' }];
			expect(sortByKeyWithUnderscoreFirst(arr, 'name')).toEqual(arr);
		});
	});

	describe('omit', () => {
		it('should omit specified properties', () => {
			const obj = { a: 1, b: 2, c: 3 };
			const result = omit(obj, ['b']);
			expect(result).toEqual({ a: 1, c: 3 });
			expect(result.b).toBeUndefined();
		});

		it('should omit multiple properties', () => {
			const obj = { a: 1, b: 2, c: 3, d: 4 };
			const result = omit(obj, ['b', 'd']);
			expect(result).toEqual({ a: 1, c: 3 });
		});

		it('should return new object without modifying original', () => {
			const obj = { a: 1, b: 2 };
			const result = omit(obj, ['b']);
			expect(obj).toEqual({ a: 1, b: 2 });
			expect(result).toEqual({ a: 1 });
		});

		it('should handle empty props array', () => {
			const obj = { a: 1, b: 2 };
			expect(omit(obj, [])).toEqual(obj);
		});

		it('should handle non-existent properties', () => {
			const obj = { a: 1, b: 2 };
			const result = omit(obj, ['c', 'd']);
			expect(result).toEqual({ a: 1, b: 2 });
		});
	});

	describe('invert', () => {
		it('should invert object key-value pairs', () => {
			const obj = { a: 1, b: 2, c: 3 };
			const result = invert(obj);
			expect(result.get(1)).toBe('a');
			expect(result.get(2)).toBe('b');
			expect(result.get(3)).toBe('c');
		});

		it('should handle empty object', () => {
			const result = invert({});
			expect(result.size).toBe(0);
		});

		it('should handle duplicate values (last key wins)', () => {
			const obj = { a: 1, b: 1, c: 2 };
			const result = invert(obj);
			expect(result.get(1)).toBe('b');
			expect(result.get(2)).toBe('c');
		});
	});

	describe('numberFormatWithComma', () => {
		it('should format number with commas', () => {
			// Use locale-aware comparison since Intl.NumberFormat uses system locale
			// Indian numbering: 10,00,000 (lakhs/crores)
			// Western numbering: 1,000,000 (thousands/millions)
			const result1 = numberFormatWithComma(1000);
			const result2 = numberFormatWithComma(1000000);
			
			// Verify it's a formatted number string
			expect(typeof result1).toBe('string');
			expect(typeof result2).toBe('string');
			
			// Check that numbers are formatted (contain digits)
			// Remove separators for comparison
			const cleanResult1 = result1.replace(/[,\s]/g, '');
			const cleanResult2 = result2.replace(/[,\s]/g, '');
			expect(cleanResult1).toBe('1000');
			expect(cleanResult2).toBe('1000000');
			
			// Verify specific formats based on locale
			// Accept both Western (1,000,000) and Indian (10,00,000) formats
			expect(result1 === '1,000' || result1 === '1 000' || result1 === '1000').toBe(true);
			expect(result2 === '1,000,000' || result2 === '10,00,000' || result2 === '1 000 000' || result2 === '1000000').toBe(true);
		});

		it('should handle zero', () => {
			expect(numberFormatWithComma(0)).toBe('0');
		});

		it('should handle negative numbers', () => {
			expect(numberFormatWithComma(-1000)).toBe('-1,000');
		});

		it('should handle decimal numbers', () => {
			const result = numberFormatWithComma(1234.56);
			expect(result).toContain('1,234');
		});
	});

	describe('numberFormatWithBytes', () => {
		it('should format bytes', () => {
			expect(numberFormatWithBytes(0)).toBe('0 Bytes');
			expect(numberFormatWithBytes(1024)).toContain('KB');
			expect(numberFormatWithBytes(1024 * 1024)).toContain('MB');
			expect(numberFormatWithBytes(1024 * 1024 * 1024)).toContain('GB');
		});

		it('should handle zero', () => {
			expect(numberFormatWithBytes(0)).toBe('0 Bytes');
		});

		it('should calculate i when number is not zero (line 194 else branch)', () => {
			// Line 194: let i = number == 0 ? 0 : Math.floor(Math.log(number) / Math.log(1024));
			// The else branch is when number != 0 (not using === but ==)
			// Need to ensure number != 0 to hit the else branch
			const result1 = numberFormatWithBytes(1024);
			expect(result1).toContain('KB');
			
			// Test with different values to ensure the else branch is hit
			const result2 = numberFormatWithBytes(1);
			expect(result2).toContain('Bytes');
			
			const result3 = numberFormatWithBytes(2048);
			expect(result3).toContain('KB');
		});

		it('should format large numbers with comma when > 8 units', () => {
			const largeNumber = Math.pow(1024, 9);
			const result = numberFormatWithBytes(largeNumber);
			expect(result).toContain(',');
		});

		it('should return number as is for negative values', () => {
			expect(numberFormatWithBytes(-1)).toBe(-1);
		});

		it('should handle TB, PB, EB, ZB, YB', () => {
			expect(numberFormatWithBytes(Math.pow(1024, 4))).toContain('TB');
			expect(numberFormatWithBytes(Math.pow(1024, 5))).toContain('PB');
			expect(numberFormatWithBytes(Math.pow(1024, 6))).toContain('EB');
			expect(numberFormatWithBytes(Math.pow(1024, 7))).toContain('ZB');
			expect(numberFormatWithBytes(Math.pow(1024, 8))).toContain('YB');
		});
	});

	describe('without', () => {
		it('should exclude specified values', () => {
			expect(without([1, 2, 3, 4], 2, 4)).toEqual([1, 3]);
		});

		it('should handle multiple exclusions', () => {
			expect(without([1, 2, 3, 4, 5], 1, 3, 5)).toEqual([2, 4]);
		});

		it('should handle empty array', () => {
			expect(without([], 1, 2)).toEqual([]);
		});

		it('should handle no exclusions', () => {
			expect(without([1, 2, 3])).toEqual([1, 2, 3]);
		});

		it('should handle non-existent values', () => {
			expect(without([1, 2, 3], 4, 5)).toEqual([1, 2, 3]);
		});
	});

	describe('union', () => {
		it('should combine arrays and return unique values', () => {
			expect(union([1, 2], [3, 4])).toEqual([1, 2, 3, 4]);
		});

		it('should remove duplicates', () => {
			expect(union([1, 2], [2, 3])).toEqual([1, 2, 3]);
		});

		it('should handle multiple arrays', () => {
			expect(union([1, 2], [3, 4], [5, 6])).toEqual([1, 2, 3, 4, 5, 6]);
		});

		it('should handle empty arrays', () => {
			expect(union([], [])).toEqual([]);
		});

		it('should handle single array', () => {
			expect(union([1, 2, 3])).toEqual([1, 2, 3]);
		});
	});

	describe('customSortObj', () => {
		it('should sort object keys', () => {
			const obj = { z: 1, a: 2, m: 3 };
			const result = customSortObj(obj);
			expect(Object.keys(result)).toEqual(['a', 'm', 'z']);
		});

		it('should return empty object for empty input', () => {
			mockIsEmpty.mockReturnValueOnce(true);
			expect(customSortObj({})).toEqual({});
		});

		it('should preserve values', () => {
			const obj = { z: 1, a: 2 };
			const result = customSortObj(obj);
			expect(result.a).toBe(2);
			expect(result.z).toBe(1);
		});

		it('should handle already sorted object', () => {
			const obj = { a: 1, b: 2, c: 3 };
			const result = customSortObj(obj);
			expect(Object.keys(result)).toEqual(['a', 'b', 'c']);
		});
	});

	describe('isEmptyValueCheck', () => {
		it('should return true for null', () => {
			expect(isEmptyValueCheck(null)).toBe(true);
		});

		it('should return true for undefined', () => {
			expect(isEmptyValueCheck(undefined)).toBe(true);
		});

		it('should return true for empty string', () => {
			expect(isEmptyValueCheck('')).toBe(true);
			expect(isEmptyValueCheck('   ')).toBe(true);
		});

		it('should return true for empty array', () => {
			expect(isEmptyValueCheck([])).toBe(true);
		});

		it('should return true for empty object', () => {
			expect(isEmptyValueCheck({})).toBe(true);
		});

		it('should return false for non-empty string', () => {
			expect(isEmptyValueCheck('test')).toBe(false);
		});

		it('should return false for non-empty array', () => {
			expect(isEmptyValueCheck([1, 2, 3])).toBe(false);
		});

		it('should return false for non-empty object', () => {
			expect(isEmptyValueCheck({ a: 1 })).toBe(false);
		});

		it('should return false for number', () => {
			expect(isEmptyValueCheck(0)).toBe(false);
			expect(isEmptyValueCheck(123)).toBe(false);
		});

		it('should return false for boolean', () => {
			expect(isEmptyValueCheck(true)).toBe(false);
			expect(isEmptyValueCheck(false)).toBe(false);
		});
	});
});
