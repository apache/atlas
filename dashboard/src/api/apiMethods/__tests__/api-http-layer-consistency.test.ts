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

import * as fs from 'fs'
import * as path from 'path'

/**
 * All dashboard HTTP calls must go through `fetchApi` so status handling
 * (401 / 403 / 404 / 419 / 5xx / network) stays consistent in the UI.
 * Most modules use `_get` / `_post` from `apiMethod.ts`, which delegates to
 * `fetchApi`.
 */
const API_METHODS_ROOT = path.join(__dirname, '..')

const IGNORED_FILES = new Set(['fetchApi.ts', 'apiMethod.ts'])

describe('API HTTP layer consistency', () => {
	it('each apiMethods/*.ts module imports fetchApi or apiMethod (not ad-hoc axios)', () => {
		const entries = fs.readdirSync(API_METHODS_ROOT, { withFileTypes: true })
		const offenders: string[] = []

		for (const entry of entries) {
			if (!entry.isFile() || !entry.name.endsWith('.ts')) {
				continue
			}
			if (IGNORED_FILES.has(entry.name)) {
				continue
			}

			const fullPath = path.join(API_METHODS_ROOT, entry.name)
			const source = fs.readFileSync(fullPath, 'utf8')

			const usesFetchApi = /from\s+['"]\.\/fetchApi['"]/.test(source)
			const usesApiMethod = /from\s+['"]\.\/apiMethod['"]/.test(source)

			if (!usesFetchApi && !usesApiMethod) {
				offenders.push(entry.name)
				continue
			}

			// Guard against bypassing the shared layer with a direct axios import
			if (/from\s+['"]axios['"]/.test(source)) {
				offenders.push(`${entry.name} (imports axios directly)`)
			}
		}

		expect(offenders).toEqual([])
	})
})
