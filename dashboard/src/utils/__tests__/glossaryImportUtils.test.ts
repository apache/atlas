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
	buildGenericImportFailureSummary,
	buildGlossaryImportFailureSummary,
	formatGenericImportFailure,
	formatGlossaryImportFailure,
	formatImportFailureForDisplay,
	getGlossaryImportTermLabel,
	getImportFailureToastMessage
} from "../glossaryImportUtils";

describe("glossaryImportUtils", () => {
	it("formats failure with glossary term label when both names exist", () => {
		expect(
			formatGlossaryImportFailure({
				childObjectName: "Patient",
				parentObjectName: "Healthcare Glossary",
				remarks: "Reference not found"
			})
		).toBe("Patient@Healthcare Glossary: Reference not found");
	});

	it("formats failure with childObjectName only (same-glossary shorthand)", () => {
		expect(
			formatGlossaryImportFailure({
				childObjectName: "Patient",
				remarks: "Invalid relation"
			})
		).toBe("Patient: Invalid relation");
		expect(getGlossaryImportTermLabel({ childObjectName: "Patient" })).toBe(
			"Patient"
		);
	});

	it("formats failure with remarks only (no term names)", () => {
		expect(
			formatGlossaryImportFailure({
				remarks: "Bad row"
			})
		).toBe("Unknown term: Bad row");
	});

	it("falls back to Import failed when remarks are empty", () => {
		expect(formatGlossaryImportFailure({ childObjectName: "Patient" })).toBe(
			"Patient: Import failed"
		);
		expect(formatGenericImportFailure({})).toBe("Import failed");
	});

	it("builds import summary with success and failure counts", () => {
		expect(
			buildGlossaryImportFailureSummary({
				successImportInfoList: [{ childObjectName: "A" }],
				failedImportInfoList: [
					{
						childObjectName: "Patient",
						parentObjectName: "Healthcare Glossary",
						remarks: "Invalid relation"
					}
				]
			})
		).toBe(
			"Glossary import completed with 1 failure(s) out of 2 term(s). See error details."
		);
	});

	it("builds summary with failures only (no successes)", () => {
		expect(
			buildGlossaryImportFailureSummary({
				failedImportInfoList: [{ remarks: "Bad row" }]
			})
		).toBe(
			"Glossary import completed with 1 failure(s) out of 1 term(s). See error details."
		);
	});

	it("builds summary with empty success and failure lists", () => {
		expect(buildGlossaryImportFailureSummary({})).toBe(
			"Glossary import completed with 0 failure(s) out of 0 term(s). See error details."
		);
	});

	it("uses generic import helpers for business metadata", () => {
		const response = {
			successImportInfoList: [{ remarks: "ok" }],
			failedImportInfoList: [
				{
					parentObjectName: "guid-123",
					childObjectName: "attr1",
					remarks: "Invalid attribute"
				}
			]
		};

		expect(getImportFailureToastMessage(false, response)).toBe(
			"Invalid attribute"
		);
		expect(formatImportFailureForDisplay(false, response.failedImportInfoList[0])).toBe(
			"Invalid attribute"
		);
		expect(buildGenericImportFailureSummary(response)).toBe(
			"Import completed with 1 failure(s) out of 2 item(s). See error details."
		);
	});

	it("uses generic summary toast for multiple business metadata failures", () => {
		expect(
			getImportFailureToastMessage(false, {
				failedImportInfoList: [
					{ remarks: "First error" },
					{ remarks: "Second error" }
				]
			})
		).toBe(
			"Import completed with 2 failure(s) out of 2 item(s). See error details."
		);
	});
});
