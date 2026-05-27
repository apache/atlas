/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 * lint-staged config: Git reports paths as dashboard/src/... from repo root;
 * ESLint runs with cwd = dashboard, so strip the dashboard/ prefix.
 */

export default {
	'dashboard/src/**/*.{ts,tsx}': (filenames) => {
		if (filenames.length === 0) {
			return process.platform === 'win32' ? 'node -e "process.exit(0)"' : 'true'
		}
		const relative = filenames.map((f) => f.replace(/^dashboard\//, ''))
		return `eslint --max-warnings 200 ${relative.join(' ')}`
	},
}
