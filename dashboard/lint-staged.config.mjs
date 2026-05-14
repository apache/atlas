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
