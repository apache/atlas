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

#!/usr/bin/env node

/**
 * Ensures platform-specific native binaries (Rollup, esbuild) are present.
 *
 * npm ci on one OS can omit optional native deps for other platforms
 * (https://github.com/npm/cli/issues/4828). This script installs the
 * missing package for the current platform after install/ci.
 *
 * Fast path: when optionalDependencies already installed native packages,
 * exits immediately without reading package-lock.json or running ldd.
 */
import { execSync } from 'node:child_process'
import { chmodSync, existsSync, readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { arch, platform } from 'node:process'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')

const ROLLUP_NATIVE = {
	'darwin-arm64': '@rollup/rollup-darwin-arm64',
	'darwin-x64': '@rollup/rollup-darwin-x64',
	'win32-arm64': '@rollup/rollup-win32-arm64-msvc',
	'win32-x64': '@rollup/rollup-win32-x64-msvc',
}

const LINUX_ROLLUP_CANDIDATES = {
	x64: ['@rollup/rollup-linux-x64-gnu', '@rollup/rollup-linux-x64-musl'],
	arm64: [
		'@rollup/rollup-linux-arm64-gnu',
		'@rollup/rollup-linux-arm64-musl',
	],
}

const ESBUILD_NATIVE = {
	'darwin-arm64': '@esbuild/darwin-arm64',
	'darwin-x64': '@esbuild/darwin-x64',
	'linux-arm64': '@esbuild/linux-arm64',
	'linux-x64': '@esbuild/linux-x64',
	'win32-arm64': '@esbuild/win32-arm64',
	'win32-x64': '@esbuild/win32-x64',
}

const packageExists = (name) =>
	name && existsSync(join(root, 'node_modules', name))

const getInstalledRollupPackage = () => {
	if (platform === 'linux') {
		const candidates = LINUX_ROLLUP_CANDIDATES[arch] ?? []
		return candidates.find((name) => packageExists(name))
	}

	const name = ROLLUP_NATIVE[`${platform}-${arch}`]
	return packageExists(name) ? name : undefined
}

const getEsbuildPackage = () => ESBUILD_NATIVE[`${platform}-${arch}`]

const chmodEsbuild = (name) => {
	const binName = platform === 'win32' ? 'esbuild.exe' : 'esbuild'
	const binPath = join(root, 'node_modules', name, 'bin', binName)
	if (!existsSync(binPath)) {
		return
	}

	try {
		chmodSync(binPath, 0o755)
	} catch {
		// Ignore chmod errors (e.g. on Windows or read-only mounts).
	}
}

// Fast path: skip lockfile parse and musl detection when binaries are present.
const esbuildPackage = getEsbuildPackage()
const installedRollup = getInstalledRollupPackage()
if (installedRollup && (!esbuildPackage || packageExists(esbuildPackage))) {
	if (esbuildPackage) {
		chmodEsbuild(esbuildPackage)
	}
	process.exit(0)
}

let lockCache
const readLock = () => {
	if (lockCache !== undefined) {
		return lockCache
	}

	const lockPath = join(root, 'package-lock.json')
	if (!existsSync(lockPath)) {
		lockCache = null
		return lockCache
	}

	lockCache = JSON.parse(readFileSync(lockPath, 'utf8'))
	return lockCache
}

const getEsbuildVersion = () => {
	const lock = readLock()
	return lock?.packages?.['node_modules/esbuild']?.version ?? '0.25.4'
}

const getRollupVersion = () => {
	const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
	if (pkg.overrides?.rollup) {
		return pkg.overrides.rollup
	}

	const lock = readLock()
	return lock?.packages?.['node_modules/rollup']?.version ?? '4.59.0'
}

const isLinuxMusl = () => {
	if (platform !== 'linux') {
		return false
	}

	try {
		const report = process.report?.getReport()
		if (report?.header?.glibcVersionRuntime) {
			return false
		}
	} catch {
		// Fall through to ldd check.
	}

	try {
		const output = execSync('ldd /bin/sh 2>&1 || true', {
			encoding: 'utf8',
			stdio: ['ignore', 'pipe', 'pipe'],
		})
		return output.includes('musl')
	} catch {
		return false
	}
}

const getRollupPackageToInstall = () => {
	if (platform === 'linux') {
		const musl = isLinuxMusl()
		if (arch === 'x64') {
			return musl
				? '@rollup/rollup-linux-x64-musl'
				: '@rollup/rollup-linux-x64-gnu'
		}
		if (arch === 'arm64') {
			return musl
				? '@rollup/rollup-linux-arm64-musl'
				: '@rollup/rollup-linux-arm64-gnu'
		}
	}

	return ROLLUP_NATIVE[`${platform}-${arch}`]
}

const ensurePackage = (name, version) => {
	if (packageExists(name)) {
		return
	}

	console.log(`Installing missing native binary: ${name}@${version}`)
	execSync(
		`npm install --no-save --no-package-lock --prefer-offline --no-audit --no-fund --ignore-scripts ${name}@${version}`,
		{ cwd: root, stdio: 'inherit' }
	)
}

const rollupPackage = getRollupPackageToInstall()
if (rollupPackage) {
	ensurePackage(rollupPackage, getRollupVersion())
}

if (esbuildPackage) {
	ensurePackage(esbuildPackage, getEsbuildVersion())
	chmodEsbuild(esbuildPackage)
}
