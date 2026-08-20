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

export interface EnumOption {
	label: string;
	value: string;
}

export interface EnumDef {
	name: string;
	elementDefs?: Array<{ value: string }>;
}

export const getInnerTypeName = (typeName: string): string => {
	if (!typeName || typeName.indexOf("array<") !== 0) {
		return typeName;
	}

	const match = typeName.match(/array<(.*)>/);
	return match?.[1] || typeName;
};

export const isArrayTypeName = (typeName: string): boolean =>
	Boolean(typeName && typeName.indexOf("array<") === 0);

export const isEnumTypeName = (
	typeName: string,
	enumDefs: EnumDef[] | undefined
): boolean => {
	if (!typeName || !enumDefs?.length) {
		return false;
	}

	const innerTypeName = getInnerTypeName(typeName);
	return enumDefs.some((enumDef) => enumDef.name === innerTypeName);
};

export const buildEnumOptionsForTypeName = (
	typeName: string,
	enumDefs: EnumDef[] | undefined
): EnumOption[] => {
	if (typeName === "array<boolean>") {
		return [
			{ label: "true", value: "true" },
			{ label: "false", value: "false" }
		];
	}

	if (!enumDefs?.length) {
		return [];
	}

	const innerTypeName = getInnerTypeName(typeName);
	const foundEnumType = enumDefs.find((enumDef) => enumDef.name === innerTypeName);
	const elementDefs = foundEnumType?.elementDefs || [];

	return elementDefs.map((elementDef) => ({
		label: elementDef.value,
		value: elementDef.value
	}));
};

export const normalizeMultiEnumValue = (
	value: unknown,
	options: EnumOption[]
): EnumOption[] => {
	if (!Array.isArray(value)) {
		return [];
	}

	return value.map((entry) => {
		if (entry && typeof entry === "object" && "value" in entry) {
			const optionValue = String((entry as EnumOption).value);
			return (
				options.find((option) => option.value === optionValue) || {
					label: optionValue,
					value: optionValue
				}
			);
		}

		const optionValue = String(entry);
		return (
			options.find((option) => option.value === optionValue) || {
				label: optionValue,
				value: optionValue
			}
		);
	});
};

export const serializeMultiEnumValue = (value: unknown): string[] => {
	if (!Array.isArray(value)) {
		return [];
	}

	return value.map((entry) => {
		if (entry && typeof entry === "object" && "value" in entry) {
			return String((entry as EnumOption).value);
		}

		return String(entry);
	});
};

export const getEnumOptionLabel = (option: EnumOption | string): string => {
	if (typeof option === "string") {
		return option;
	}

	return option.label;
};

export const areEnumOptionsEqual = (
	option: EnumOption,
	value: EnumOption | string
): boolean => {
	if (typeof value === "string") {
		return option.value === value;
	}

	return option.value === value.value;
};
