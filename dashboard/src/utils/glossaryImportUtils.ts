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

export interface GlossaryImportFailure {
	childObjectName?: string;
	parentObjectName?: string;
	remarks?: string;
	rowNumber?: number;
}

export interface GlossaryImportResponse {
	failedImportInfoList?: GlossaryImportFailure[];
	successImportInfoList?: GlossaryImportFailure[];
}

/** Glossary-only: term label for TermName@GlossaryName when both names exist. */
export const getGlossaryImportTermLabel = (
	failure: GlossaryImportFailure
): string => {
	if (failure.childObjectName && failure.parentObjectName) {
		return `${failure.childObjectName}@${failure.parentObjectName}`;
	}
	if (failure.childObjectName) {
		return failure.childObjectName;
	}
	return "Unknown term";
};

/** Glossary-only: one formatted failure line (term label + reason). */
export const formatGlossaryImportFailure = (
	failure: GlossaryImportFailure
): string => {
	return `${getGlossaryImportTermLabel(failure)}: ${failure.remarks || "Import failed"}`;
};

/** Glossary-only: summary toast for bulk glossary term import failures. */
export const buildGlossaryImportFailureSummary = (
	response: GlossaryImportResponse
): string => {
	const failedCount = response.failedImportInfoList?.length || 0;
	const successCount = response.successImportInfoList?.length || 0;
	const totalCount = failedCount + successCount;

	return `Glossary import completed with ${failedCount} failure(s) out of ${totalCount} term(s). See error details.`;
};

/** Generic import (e.g. Business Metadata): use raw remarks, not glossary @ labels. */
export const formatGenericImportFailure = (
	failure: GlossaryImportFailure
): string => {
	return failure.remarks || "Import failed";
};

/** Generic import summary when multiple failures exist (non-glossary imports). */
export const buildGenericImportFailureSummary = (
	response: GlossaryImportResponse
): string => {
	const failedCount = response.failedImportInfoList?.length || 0;
	const successCount = response.successImportInfoList?.length || 0;
	const totalCount = failedCount + successCount;

	return `Import completed with ${failedCount} failure(s) out of ${totalCount} item(s). See error details.`;
};

/** Pick toast message based on import type — glossary vs shared/BM dialog. */
export const getImportFailureToastMessage = (
	isGlossaryImport: boolean,
	response: GlossaryImportResponse
): string => {
	if (isGlossaryImport) {
		return buildGlossaryImportFailureSummary(response);
	}

	const failedList = response.failedImportInfoList;
	if (failedList && failedList.length === 1) {
		return failedList[0]?.remarks ?? "Import failed";
	}

	return buildGenericImportFailureSummary(response);
};

/** Pick error-detail line based on import type. */
export const formatImportFailureForDisplay = (
	isGlossaryImport: boolean,
	failure: GlossaryImportFailure
): string => {
	if (isGlossaryImport) {
		return formatGlossaryImportFailure(failure);
	}
	return formatGenericImportFailure(failure);
};
