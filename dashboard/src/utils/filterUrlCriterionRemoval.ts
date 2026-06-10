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

import { attributeFilter } from "./CommonViewFunction";

export type RemoveCriterionResult =
	| { kind: "empty" }
	| { kind: "url"; url: string }
	| { kind: "singleTypeName"; typeName: string };

type ApiFilterCriterionRow = {
	attributeName: string;
	operator: string;
	attributeValue: string;
	type?: string;
};

/** Narrowing shape for `attributeFilter.extractUrl(..., { apiObj: true })`. */
export type ParsedApiFilter = {
	condition: string;
	criterion: ApiFilterCriterionRow[];
};

/**
 * Drops one criterion from a serialized filter URL (entity / tag / relationship).
 * When allowTypeParamSimplification is true and exactly one __typeName eq remains,
 * returns singleTypeName so the UI can use type=<typedef> (matches sidebar search).
 */
export const removeCriterionFromFilterUrl = (
	filterUrl: string,
	index: number,
	allowTypeParamSimplification = false
): RemoveCriterionResult | null => {
	if (!filterUrl || index < 0) {
		return null;
	}
	const parsed = attributeFilter.extractUrl({
		value: filterUrl,
		apiObj: true
	}) as ParsedApiFilter | null;
	if (!parsed?.criterion || !Array.isArray(parsed.criterion)) {
		return null;
	}
	const crit = [...parsed.criterion];
	if (index >= crit.length) {
		return null;
	}
	crit.splice(index, 1);
	if (crit.length === 0) {
		return { kind: "empty" };
	}
	if (
		allowTypeParamSimplification &&
		crit.length === 1 &&
		crit[0].attributeName === "__typeName" &&
		String(crit[0].operator).toLowerCase() === "eq"
	) {
		return { kind: "singleTypeName", typeName: String(crit[0].attributeValue) };
	}
	const url = attributeFilter.generateUrl({
		value: { condition: parsed.condition, criterion: crit }
	});
	if (!url) {
		return { kind: "empty" };
	}
	return { kind: "url", url };
};
