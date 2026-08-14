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

import { getApiErrorToastMessage } from '../apiErrorToastMessage'

describe('getApiErrorToastMessage', () => {
	it('returns null for 403 — fetchApi already shows forbidden toast', () => {
		const message = getApiErrorToastMessage({
			response: { status: 403, data: { errorMessage: 'No access' } },
		})
		expect(message).toBeNull()
	})

	it('prefers errorMessage from JSON body', () => {
		expect(
			getApiErrorToastMessage({
				response: {
					status: 500,
					data: { errorMessage: '  Atlas error  ' },
				},
			})
		).toBe('  Atlas error  ')
	})

	it('uses msgDesc when errorMessage is absent', () => {
		expect(
			getApiErrorToastMessage({
				response: { status: 404, data: { msgDesc: 'Not found desc' } },
			})
		).toBe('Not found desc')
	})

	it('uses string response body when present', () => {
		expect(
			getApiErrorToastMessage({
				response: { status: 502, data: 'Plain text error' },
			})
		).toBe('Plain text error')
	})

	it('returns fallback when body has no known fields', () => {
		expect(
			getApiErrorToastMessage({
				response: { status: 400, data: { other: true } },
			})
		).toBe('Invalid JSON response from server')
	})

	it('skips blank errorMessage and falls back', () => {
		expect(
			getApiErrorToastMessage({
				response: {
					status: 422,
					data: { errorMessage: '   ', msgDesc: 'From msgDesc' },
				},
			})
		).toBe('From msgDesc')
	})

	it('handles non-axios-shaped error safely', () => {
		expect(getApiErrorToastMessage(undefined)).toBe(
			'Invalid JSON response from server'
		)
		expect(getApiErrorToastMessage({})).toBe(
			'Invalid JSON response from server'
		)
	})
})
