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
 * Dependency & Dependabot governance (opt-in).
 *
 * Run: npm run test:governance
 * Or:  RUN_DEPENDENCY_GOVERNANCE=1 npm test
 *
 * @jest-environment node
 */

import { execSync } from 'child_process'
import fs from 'fs'
import path from 'path'
import semver from 'semver'

/** GitHub Dependabot alert (subset of REST API fields). */
interface DependabotAlert {
	number?: number
	state?: string
	dependency?: {
		package?: { ecosystem?: string; name?: string }
		manifest_path?: string
	}
	security_advisory?: {
		severity?: string
		cve_id?: string
		summary?: string
	}
	security_vulnerability?: {
		package?: { ecosystem?: string; name?: string }
		vulnerable_version_range?: string
		first_patched_version?: string | null
	}
}

interface PackageJson {
	dependencies?: Record<string, string>
	devDependencies?: Record<string, string>
	overrides?: Record<string, string>
	name?: string
}

const SEVERITY_ORDER: Record<string, number> = {
	critical: 0,
	high: 1,
	moderate: 2,
	low: 3,
	unknown: 99,
}

const dashboardRoot = path.resolve(__dirname, '..', '..', '..')
const packageJsonPath = path.join(dashboardRoot, 'package.json')

const readPackageJson = (): PackageJson => {
	const raw = fs.readFileSync(packageJsonPath, 'utf8')
	return JSON.parse(raw) as PackageJson
}

const normalizeAlertsPayload = (data: unknown): DependabotAlert[] => {
	if (Array.isArray(data)) {
		return data as DependabotAlert[]
	}
	if (data && typeof data === 'object' && Array.isArray((data as { alerts?: unknown }).alerts)) {
		return (data as { alerts: DependabotAlert[] }).alerts
	}
	if (data && typeof data === 'object' && Array.isArray((data as { data?: unknown }).data)) {
		return (data as { data: DependabotAlert[] }).data
	}
	throw new Error('Dependabot export: expected array or { alerts: [] } / { data: [] }')
}

const isDashboardFrontendAlert = (
	alert: DependabotAlert,
	pkg: PackageJson,
): boolean => {
	const eco = alert.dependency?.package?.ecosystem
	if (eco !== 'npm') {
		return false
	}
	const name = alert.dependency?.package?.name
	if (!name) {
		return false
	}
	const inDirect =
		Boolean(pkg.dependencies?.[name]) || Boolean(pkg.devDependencies?.[name])
	const manifest = alert.dependency?.manifest_path ?? ''
	const manifestMatches =
		manifest.includes('dashboard') ||
		manifest.endsWith('dashboard/package.json') ||
		manifest === 'package.json'
	return inDirect || manifestMatches
}

const sortAlertsByPriority = (alerts: DependabotAlert[]): DependabotAlert[] => {
	return [...alerts].sort((a, b) => {
		const sa = (a.security_advisory?.severity ?? 'unknown').trim().toLowerCase()
		const sb = (b.security_advisory?.severity ?? 'unknown').trim().toLowerCase()
		const oa = SEVERITY_ORDER[sa] ?? 50
		const ob = SEVERITY_ORDER[sb] ?? 50
		if (oa !== ob) {
			return oa - ob
		}
		return (a.number ?? 0) - (b.number ?? 0)
	})
}

const getDeclaredVersion = (pkg: PackageJson, packageName: string): string | null => {
	const v =
		pkg.dependencies?.[packageName] ?? pkg.devDependencies?.[packageName]
	if (!v) {
		return null
	}
	// Strip common npm prefixes for git / workspace (governance focuses on semver)
	const cleaned = v.replace(/^[\^~]/, '')
	return cleaned || null
}

const isRemediatedForAlert = (
	declaredRange: string | null,
	firstPatched: string | null | undefined,
): boolean => {
	if (!declaredRange || !firstPatched) {
		return false
	}
	const patchedCoerced = semver.coerce(firstPatched)
	if (!patchedCoerced) {
		return false
	}
	// Exact or simple range: pick minimum satisfied version from range
	let current: semver.SemVer | null = semver.minVersion(declaredRange)
	if (!current) {
		current = semver.coerce(declaredRange)
	}
	if (!current) {
		return false
	}
	return semver.gte(current, patchedCoerced)
}

interface RemediationPlan {
	alertNumber?: number
	packageName: string
	severity: string
	cveId?: string
	manifestPath?: string
	declaredVersion: string | null
	firstPatchedVersion: string | null | undefined
	remediated: boolean
	fixSteps: string[]
}

const buildRemediationPlan = (
	alert: DependabotAlert,
	pkg: PackageJson,
): RemediationPlan => {
	const packageName = alert.dependency?.package?.name ?? 'unknown'
	const declared = getDeclaredVersion(pkg, packageName)
	const patched = alert.security_vulnerability?.first_patched_version
	const remediated = isRemediatedForAlert(declared, patched)
	const fixSteps: string[] = []
	if (!declared) {
		fixSteps.push(
			`Add or bump direct dependency "${packageName}" in dashboard/package.json (Dependabot targets this repo).`,
		)
		if (patched) {
			fixSteps.push(`Set version to at least ${patched} (first patched).`)
		}
	} else if (!remediated && patched) {
		fixSteps.push(
			`Bump "${packageName}" from "${declared}" to >= ${patched} in dependencies or devDependencies.`,
		)
		fixSteps.push(
			'If the issue is transitive only, add an npm "overrides" entry and re-run npm install.',
		)
		fixSteps.push('Run: npm run lint && npm test && npm run build')
	} else if (!patched) {
		fixSteps.push(
			'No first_patched_version in alert; check GHSA/CVE page and choose a safe version manually.',
		)
	}
	return {
		alertNumber: alert.number,
		packageName,
		severity: alert.security_advisory?.severity ?? 'unknown',
		cveId: alert.security_advisory?.cve_id,
		manifestPath: alert.dependency?.manifest_path,
		declaredVersion: declared,
		firstPatchedVersion: patched,
		remediated,
		fixSteps,
	}
}

const assertOpenAlertsRemediated = (
	pkg: PackageJson,
	alerts: DependabotAlert[],
): void => {
	const open = alerts.filter((a) => (a.state ?? 'open').toLowerCase() === 'open')
	const frontend = open.filter((a) => isDashboardFrontendAlert(a, pkg))
	const sorted = sortAlertsByPriority(frontend)
	const bad: string[] = []
	for (const alert of sorted) {
		const plan = buildRemediationPlan(alert, pkg)
		if (!plan.remediated) {
			bad.push(
				[
					`[${plan.severity}] ${plan.packageName}`,
					plan.cveId ? `CVE/GitHub: ${plan.cveId}` : '',
					`Declared: ${plan.declaredVersion ?? '(not a direct dep)'}`,
					`First patched: ${plan.firstPatchedVersion ?? 'unknown'}`,
					'Plan:',
					...plan.fixSteps.map((s) => `  - ${s}`),
				]
					.filter(Boolean)
					.join('\n'),
			)
		}
	}
	if (bad.length > 0) {
		throw new Error(
			`Dependabot governance: unremediated frontend-scoped alerts:\n\n${bad.join('\n\n---\n\n')}`,
		)
	}
}

describe('Dependency & Dependabot governance', () => {
	jest.setTimeout(
		process.env.GOVERNANCE_SKIP_AUDIT === '1' ||
			process.env.GOVERNANCE_SKIP_AUDIT === 'true'
			? 10_000
			: 120_000,
	)

	const runAudit =
		process.env.GOVERNANCE_SKIP_AUDIT !== '1' &&
		process.env.GOVERNANCE_SKIP_AUDIT !== 'true'

	it('documents the manual upgrade + CVE checklist in the test plan', () => {
		const planPath = path.join(
			dashboardRoot,
			'docs',
			'DEPENDENCY_AND_DEPENDABOT_TEST_PLAN.md',
		)
		expect(fs.existsSync(planPath)).toBe(true)
		const text = fs.readFileSync(planPath, 'utf8')
		expect(text).toContain('npm audit')
		expect(text).toContain('Dependabot')
		expect(text).toContain('lint')
		expect(text).toContain('build')
	})

	it('loads package.json with declared dependencies', () => {
		const pkg = readPackageJson()
		expect(pkg.name).toBe('dashboard')
		expect(pkg.dependencies && Object.keys(pkg.dependencies).length).toBeGreaterThan(
			0,
		)
	})

	describe('npm audit (registry required)', () => {
		it('reports no vulnerabilities at or above GOVERNANCE_AUDIT_FAIL_LEVEL', () => {
			if (!runAudit) {
				console.warn(
					'[governance] GOVERNANCE_SKIP_AUDIT=1 — skipping npm audit subprocess',
				)
				return
			}
			let parsed: {
				metadata?: { vulnerabilities?: Record<string, number> }
				vulnerabilities?: Record<string, unknown>
			}
			try {
				const out = execSync('npm audit --json', {
					cwd: dashboardRoot,
					encoding: 'utf8',
					maxBuffer: 20 * 1024 * 1024,
					env: { ...process.env, npm_config_audit_level: undefined },
				})
				parsed = JSON.parse(out)
			} catch (err: unknown) {
				const e = err as { stdout?: string; status?: number }
				const raw = e.stdout
				if (!raw) {
					console.warn(
						'[governance] npm audit failed (network/registry). Set GOVERNANCE_SKIP_AUDIT=1 to skip.',
						err,
					)
					return
				}
				try {
					parsed = JSON.parse(raw)
				} catch {
					throw err
				}
			}

			const levelRaw =
				(process.env.GOVERNANCE_AUDIT_FAIL_LEVEL ?? 'high').toLowerCase()
			const order = ['low', 'moderate', 'high', 'critical']
			let thresholdIdx = order.indexOf(levelRaw)
			if (thresholdIdx < 0) {
				thresholdIdx = order.indexOf('high')
			}
			const counts = parsed.metadata?.vulnerabilities ?? {}
			let failing = 0
			const parts: string[] = []
			for (let i = thresholdIdx; i < order.length; i += 1) {
				const sev = order[i]
				const c = counts[sev] ?? 0
				if (c > 0) {
					failing += c
					parts.push(`${sev}: ${c}`)
				}
			}
			if (failing > 0) {
				throw new Error(
					`npm audit: ${failing} issue(s) at/above "${level}" (${parts.join(', ')}). Run npm audit fix or bump deps.`,
				)
			}
		})
	})

	describe('Dependabot export parsing & remediation plan', () => {
		it('sorts alerts critical before high before moderate', () => {
			const alerts: DependabotAlert[] = [
				{
					number: 1,
					state: 'open',
					security_advisory: { severity: 'low' },
					dependency: { package: { ecosystem: 'npm', name: 'a' } },
				},
				{
					number: 2,
					state: 'open',
					security_advisory: { severity: 'critical' },
					dependency: { package: { ecosystem: 'npm', name: 'b' } },
				},
				{
					number: 3,
					state: 'open',
					security_advisory: { severity: 'high' },
					dependency: { package: { ecosystem: 'npm', name: 'c' } },
				},
			]
			const sorted = sortAlertsByPriority(alerts)
			expect(sorted[0].dependency?.package?.name).toBe('b')
			expect(sorted[1].dependency?.package?.name).toBe('c')
			expect(sorted[2].dependency?.package?.name).toBe('a')
		})

		it('throws when a direct dependency is below first_patched_version', () => {
			const pkg: PackageJson = {
				dependencies: { axios: '1.0.0' },
			}
			const alerts: DependabotAlert[] = [
				{
					state: 'open',
					dependency: {
						package: { ecosystem: 'npm', name: 'axios' },
						manifest_path: 'dashboard/package.json',
					},
					security_advisory: { severity: 'high', cve_id: 'CVE-TEST' },
					security_vulnerability: {
						first_patched_version: '1.6.0',
						package: { ecosystem: 'npm', name: 'axios' },
					},
				},
			]
			expect(() => assertOpenAlertsRemediated(pkg, alerts)).toThrow(
				/unremediated frontend-scoped alerts/,
			)
		})

		it('sample fixture: axios advisory is satisfied by current dashboard axios', () => {
			const pkg = readPackageJson()
			const fixturePath = path.join(
				__dirname,
				'fixtures',
				'dependabot-alerts.sample.json',
			)
			const alerts = normalizeAlertsPayload(
				JSON.parse(fs.readFileSync(fixturePath, 'utf8')),
			)
			assertOpenAlertsRemediated(pkg, alerts)
		})

		it('optional DEPENDABOT_ALERTS_PATH: open frontend alerts must be remediated', () => {
			const exportPath = process.env.DEPENDABOT_ALERTS_PATH
			if (!exportPath || !fs.existsSync(exportPath)) {
				console.warn(
					'[governance] DEPENDABOT_ALERTS_PATH not set or missing — skipping live Dependabot file test',
				)
				return
			}
			const pkg = readPackageJson()
			const alerts = normalizeAlertsPayload(
				JSON.parse(fs.readFileSync(path.resolve(exportPath), 'utf8')),
			)
			assertOpenAlertsRemediated(pkg, alerts)
		})
	})

	describe('Remediation plan builder', () => {
		it('produces actionable steps when version is behind patched', () => {
			const pkg: PackageJson = { dependencies: { axios: '0.21.0' } }
			const alert: DependabotAlert = {
				number: 9,
				state: 'open',
				dependency: {
					package: { ecosystem: 'npm', name: 'axios' },
					manifest_path: 'dashboard/package.json',
				},
				security_advisory: { severity: 'high', cve_id: 'CVE-X' },
				security_vulnerability: { first_patched_version: '1.6.0' },
			}
			const plan = buildRemediationPlan(alert, pkg)
			expect(plan.remediated).toBe(false)
			expect(plan.fixSteps.join(' ')).toContain('Bump')
			expect(plan.fixSteps.join(' ')).toContain('overrides')
		})
	})
})
