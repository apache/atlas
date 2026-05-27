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
 * License skip / scan rules for dashboardv2 and docs (dashboard uses dashboard/scripts).
 */

/** @param {string} repoRel forward slashes */
export const shouldSkipLicenseDashboardv2 = (repoRel) => {
	if (!repoRel.startsWith('dashboardv2/')) return true
	const r = repoRel.slice('dashboardv2/'.length)
	if (
		r.startsWith('node_modules/') ||
		r.startsWith('bin/') ||
		r.includes('/node_modules/') ||
		r.includes('/external_lib/')
	) {
		return true
	}
	if (r.endsWith('.min.js') || r.endsWith('.map')) return true
	if (r.endsWith('package-lock.json') || r === 'package.json') return true
	return false
}

/** @param {string} repoRel */
export const shouldSkipLicenseDocs = (repoRel) => {
	if (!repoRel.startsWith('docs/')) return true
	const r = repoRel.slice('docs/'.length)
	if (
		r.startsWith('node_modules/') ||
		r.startsWith('site/') ||
		r.startsWith('bin/') ||
		r.startsWith('docz-lib/') ||
		r.includes('/node_modules/')
	) {
		return true
	}
	if (
		r.endsWith('package-lock.json') ||
		r === 'package.json' ||
		r.endsWith('.png') ||
		r.endsWith('.svg') ||
		r.endsWith('.woff') ||
		r.endsWith('.ico')
	) {
		return true
	}
	return false
}
