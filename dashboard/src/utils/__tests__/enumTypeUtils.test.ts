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
	areEnumOptionsEqual,
	buildEnumOptionsForTypeName,
	getInnerTypeName,
	isArrayTypeName,
	isEnumTypeName,
	normalizeMultiEnumValue,
	serializeMultiEnumValue
} from "@utils/enumTypeUtils";

const mockEnumDefs = [
	{
		name: "adls_gen2_replication",
		elementDefs: [
			{ value: "LRS" },
			{ value: "ZRS" },
			{ value: "GRS" },
			{ value: "GZRS" },
			{ value: "RA-GRS" }
		]
	},
	{
		name: "ozone_storage_type",
		elementDefs: [
			{ value: "RAM_DISK" },
			{ value: "SSD" },
			{ value: "DISK" },
			{ value: "ARCHIVE" }
		]
	}
];

describe("enumTypeUtils", () => {
	it("detects array enum type names", () => {
		expect(isArrayTypeName("array<adls_gen2_replication>")).toBe(true);
		expect(isArrayTypeName("adls_gen2_replication")).toBe(false);
	});

	it("extracts inner enum type from array type name", () => {
		expect(getInnerTypeName("array<adls_gen2_replication>")).toBe(
			"adls_gen2_replication"
		);
		expect(getInnerTypeName("adls_gen2_replication")).toBe(
			"adls_gen2_replication"
		);
	});

	it("builds enum options for single and multi-value enum types", () => {
		expect(
			buildEnumOptionsForTypeName("adls_gen2_replication", mockEnumDefs)
		).toEqual([
			{ label: "LRS", value: "LRS" },
			{ label: "ZRS", value: "ZRS" },
			{ label: "GRS", value: "GRS" },
			{ label: "GZRS", value: "GZRS" },
			{ label: "RA-GRS", value: "RA-GRS" }
		]);

		expect(
			buildEnumOptionsForTypeName("array<adls_gen2_replication>", mockEnumDefs)
		).toEqual([
			{ label: "LRS", value: "LRS" },
			{ label: "ZRS", value: "ZRS" },
			{ label: "GRS", value: "GRS" },
			{ label: "GZRS", value: "GZRS" },
			{ label: "RA-GRS", value: "RA-GRS" }
		]);
	});

	it("identifies enum and non-enum type names", () => {
		expect(
			isEnumTypeName("array<adls_gen2_replication>", mockEnumDefs)
		).toBe(true);
		expect(isEnumTypeName("array<string>", mockEnumDefs)).toBe(false);
		expect(isEnumTypeName("string", mockEnumDefs)).toBe(false);
		expect(isEnumTypeName("adls_gen2_replication", undefined)).toBe(false);
	});

	it("normalizes stored string values into enum option objects", () => {
		const options = buildEnumOptionsForTypeName(
			"array<adls_gen2_replication>",
			mockEnumDefs
		);

		expect(normalizeMultiEnumValue(["LRS", "ZRS"], options)).toEqual([
			{ label: "LRS", value: "LRS" },
			{ label: "ZRS", value: "ZRS" }
		]);
	});

	it("returns empty array when multi enum value is missing or invalid", () => {
		const options = buildEnumOptionsForTypeName(
			"array<adls_gen2_replication>",
			mockEnumDefs
		);

		expect(normalizeMultiEnumValue(undefined, options)).toEqual([]);
		expect(normalizeMultiEnumValue("LRS", options)).toEqual([]);
	});

	it("serializes multi enum values for submit", () => {
		expect(serializeMultiEnumValue(["LRS", "ZRS"])).toEqual(["LRS", "ZRS"]);
		expect(
			serializeMultiEnumValue([
				{ label: "LRS", value: "LRS" },
				{ label: "ZRS", value: "ZRS" }
			])
		).toEqual(["LRS", "ZRS"]);
		expect(serializeMultiEnumValue(undefined)).toEqual([]);
	});

	it("compares enum options for autocomplete selection", () => {
		const option = { label: "LRS", value: "LRS" };

		expect(areEnumOptionsEqual(option, "LRS")).toBe(true);
		expect(areEnumOptionsEqual(option, { label: "ZRS", value: "ZRS" })).toBe(
			false
		);
	});

	it("builds ozone_storage_type enum options (positive)", () => {
		expect(
			buildEnumOptionsForTypeName("ozone_storage_type", mockEnumDefs)
		).toEqual([
			{ label: "RAM_DISK", value: "RAM_DISK" },
			{ label: "SSD", value: "SSD" },
			{ label: "DISK", value: "DISK" },
			{ label: "ARCHIVE", value: "ARCHIVE" }
		]);
	});

	it("returns false for enum type when enumDefs is empty (negative)", () => {
		expect(isEnumTypeName("ozone_storage_type", [])).toBe(false);
	});

	it("returns empty options when enum type is not found (negative)", () => {
		expect(
			buildEnumOptionsForTypeName("unknown_enum_type", mockEnumDefs)
		).toEqual([]);
	});
});
