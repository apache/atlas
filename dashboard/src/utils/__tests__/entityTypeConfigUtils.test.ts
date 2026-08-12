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
	filterEditableEntityTypes,
	isEntityTypeEditable,
	parseEditableEntityTypes
} from "@utils/entityTypeConfigUtils";

describe("entityTypeConfigUtils", () => {
	describe("parseEditableEntityTypes", () => {
		it("returns wildcard for * config (positive)", () => {
			expect(parseEditableEntityTypes("*")).toBe("*");
		});

		it("parses comma-separated entity types with spaces (positive)", () => {
			expect(parseEditableEntityTypes("hdfs_path, enumchecking")).toEqual([
				"hdfs_path",
				"enumchecking"
			]);
		});

		it("returns empty array for empty, null, or undefined config (negative)", () => {
			expect(parseEditableEntityTypes("")).toEqual([]);
			expect(parseEditableEntityTypes(null)).toEqual([]);
			expect(parseEditableEntityTypes(undefined)).toEqual([]);
		});

		it("returns empty array for whitespace-only config (negative)", () => {
			expect(parseEditableEntityTypes("   ")).toEqual([]);
		});
	});

	describe("isEntityTypeEditable", () => {
		it("allows any type when config is wildcard (positive)", () => {
			expect(isEntityTypeEditable("enumchecking", "*")).toBe(true);
			expect(isEntityTypeEditable("hdfs_path", "*")).toBe(true);
		});

		it("allows type when listed in comma-separated config (positive)", () => {
			expect(isEntityTypeEditable("enumchecking", "hdfs_path,enumchecking")).toBe(
				true
			);
		});

		it("denies type when not listed in config (negative)", () => {
			expect(isEntityTypeEditable("enumchecking", "hdfs_path")).toBe(false);
			expect(isEntityTypeEditable("DataSet", "hdfs_path,enumchecking")).toBe(
				false
			);
		});

		it("denies all types when config is empty (negative)", () => {
			expect(isEntityTypeEditable("enumchecking", "")).toBe(false);
			expect(isEntityTypeEditable("enumchecking", null)).toBe(false);
		});
	});

	describe("filterEditableEntityTypes", () => {
		const allTypes = ["hdfs_path", "enumchecking", "DataSet", "__internal"];

		it("returns all types for wildcard config (positive)", () => {
			expect(filterEditableEntityTypes(allTypes, "*")).toEqual(allTypes);
		});

		it("returns only configured types for comma-separated config (positive)", () => {
			expect(
				filterEditableEntityTypes(allTypes, "hdfs_path,enumchecking")
			).toEqual(["hdfs_path", "enumchecking"]);
		});

		it("returns empty list when config is empty (negative)", () => {
			expect(filterEditableEntityTypes(allTypes, "")).toEqual([]);
		});

		it("returns empty list when no types match config (negative)", () => {
			expect(filterEditableEntityTypes(allTypes, "unknown_type")).toEqual([]);
		});

		it("returns empty list when input type list is empty (negative)", () => {
			expect(filterEditableEntityTypes([], "hdfs_path,enumchecking")).toEqual(
				[]
			);
		});
	});
});
