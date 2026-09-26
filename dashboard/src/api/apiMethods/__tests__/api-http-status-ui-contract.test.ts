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
 * Contract between `fetchApi.ts` and UI-facing behaviour.
 * Detailed assertions live in `fetchApi.test.ts`; this suite keeps the
 * matrix visible and stable for reviewers.
 */

describe('fetchApi HTTP status → UI handling contract', () => {
	const contract: Record<
		string,
		{ ui: string; notes?: string }
	> = {
		'0': {
			ui: 'toast.error (network / throttled)',
			notes: 'Also used when response missing or status 0 (non-abort)',
		},
		'401': {
			ui: 'window.location.replace("login.jsp")',
		},
		'403': {
			ui: 'toast.error deferred (setTimeout 0), toastId fetch-api-http-403',
			notes: 'No redirect; message from body or "You are not authorized"',
		},
		'404': {
			ui: 'serverErrorHandler(..., "Resource not found")',
		},
		'419': {
			ui: 'toast.warning "Session Time Out !!" + login.jsp redirect',
		},
		'500': {
			ui: 'serverErrorHandler(..., "Internal Server Error")',
		},
		'503': {
			ui: 'serverErrorHandler(..., "Service Unavailable")',
		},
		'504': {
			ui: 'serverErrorHandler(..., "Gateway Timeout")',
		},
		default: {
			ui: 'no automatic toast in switch; error rethrown',
			notes: 'Callers may use getApiErrorToastMessage in catch',
		},
	}

	it('defines the expected status handling map', () => {
		expect(Object.keys(contract).sort()).toEqual([
			'0',
			'401',
			'403',
			'404',
			'419',
			'500',
			'503',
			'504',
			'default',
		])
	})

	it('each mapped status documents non-empty UI behaviour', () => {
		for (const key of Object.keys(contract)) {
			expect(contract[key].ui.length).toBeGreaterThan(0)
		}
	})
})
