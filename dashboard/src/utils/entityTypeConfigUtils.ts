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

export type EditableEntityTypesConfig = string[] | "*";

export const parseEditableEntityTypes = (
	editableTypesConfig: string | undefined | null
): EditableEntityTypesConfig => {
	if (!editableTypesConfig || editableTypesConfig.trim() === "") {
		return [];
	}

	const trimmedConfig = editableTypesConfig.trim();
	if (trimmedConfig === "*") {
		return "*";
	}

	return trimmedConfig
		.split(",")
		.map((typeName) => typeName.trim())
		.filter(Boolean);
};

export const isEntityTypeEditable = (
	typeName: string,
	editableTypesConfig: string | undefined | null
): boolean => {
	const parsedConfig = parseEditableEntityTypes(editableTypesConfig);

	if (parsedConfig === "*") {
		return true;
	}

	return parsedConfig.includes(typeName);
};

export const filterEditableEntityTypes = (
	allTypeNames: string[],
	editableTypesConfig: string | undefined | null
): string[] => {
	const parsedConfig = parseEditableEntityTypes(editableTypesConfig);

	if (parsedConfig === "*") {
		return allTypeNames;
	}

	return allTypeNames.filter((typeName) => parsedConfig.includes(typeName));
};
