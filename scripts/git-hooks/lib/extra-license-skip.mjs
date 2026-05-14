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
