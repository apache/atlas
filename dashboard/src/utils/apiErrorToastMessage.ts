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
 * Resolves user-visible toast text from an axios/fetchApi error.
 * @returns null when the caller must not toast (403 is handled in fetchApi).
 */
export const getApiErrorToastMessage = (error: unknown): string | null => {
	const er = error as {
		response?: { status?: number; data?: unknown };
	};
	if (er?.response?.status === 403) {
		return null;
	}
	const data = er?.response?.data;
	if (data !== null && data !== undefined && typeof data === "object") {
		const o = data as Record<string, unknown>;
		if (o.errorMessage != null && String(o.errorMessage).trim() !== "") {
			return String(o.errorMessage);
		}
		if (o.msgDesc != null && String(o.msgDesc).trim() !== "") {
			return String(o.msgDesc);
		}
	}
	if (typeof data === "string" && data.trim() !== "") {
		return data;
	}
	return "Invalid JSON response from server";
};
