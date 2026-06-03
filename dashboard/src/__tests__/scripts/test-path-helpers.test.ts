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
 * Pre-commit / pre-push test-path guard helpers (dashboard/scripts).
 *
 * @jest-environment node
 */

import path from 'path'

const dashboardRoot = path.resolve(__dirname, '../../..')

describe('test-path-helpers (EntityForm vs views/__tests__)', () => {
	let hasTestsOnDisk: (root: string, rel: string) => boolean
	let isUiSourcePath: (rel: string) => boolean

	beforeAll(async () => {
		const mod = await import('../../../scripts/lib/test-path-helpers.mjs')
		hasTestsOnDisk = mod.hasTestsOnDisk
		isUiSourcePath = mod.isUiSourcePath
	})

	it('covers src/views/Entity/EntityForm.tsx with src/views/__tests__/EntityForm.test.tsx', () => {
		expect(
			hasTestsOnDisk(dashboardRoot, 'src/views/Entity/EntityForm.tsx'),
		).toBe(true)
	})

	it('treats EntityForm under views as a guarded UI source path', () => {
		expect(isUiSourcePath('src/views/Entity/EntityForm.tsx')).toBe(true)
	})
})
