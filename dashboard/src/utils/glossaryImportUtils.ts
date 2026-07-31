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

export const formatGlossaryImportFailure = (
	failure: GlossaryImportFailure
): string => {
	const termLabel =
		failure.childObjectName && failure.parentObjectName
			? `${failure.childObjectName}@${failure.parentObjectName}`
			: failure.childObjectName || "Unknown term";

	return `${termLabel}: ${failure.remarks || "Import failed"}`;
};

export const buildGlossaryImportFailureSummary = (
	response: GlossaryImportResponse
): string => {
	const failedCount = response.failedImportInfoList?.length || 0;
	const successCount = response.successImportInfoList?.length || 0;
	const totalCount = failedCount + successCount;

	return `Glossary import completed with ${failedCount} failure(s) out of ${totalCount} term(s). See error details.`;
};
