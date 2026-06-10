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
 * Ensures platform-specific native binaries (Rollup, esbuild) are present.
 *
 * npm ci on one OS can omit optional native deps for other platforms
 * (https://github.com/npm/cli/issues/4828). This script installs the
 * missing package for the current platform after install/ci.
 */
import { execSync } from 'node:child_process'
import { chmodSync, existsSync, readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { arch, platform } from 'node:process'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
const rollupVersion = pkg.overrides?.rollup ?? '4.59.0'

const getEsbuildVersion = () => {
	const lockPath = join(root, 'package-lock.json')
	if (!existsSync(lockPath)) {
		return '0.25.4'
	}

	const lock = JSON.parse(readFileSync(lockPath, 'utf8'))
	return lock.packages?.['node_modules/esbuild']?.version ?? '0.25.4'
}

const esbuildVersion = getEsbuildVersion()

const ROLLUP_NATIVE = {
	'darwin-arm64': '@rollup/rollup-darwin-arm64',
	'darwin-x64': '@rollup/rollup-darwin-x64',
	'win32-arm64': '@rollup/rollup-win32-arm64-msvc',
	'win32-x64': '@rollup/rollup-win32-x64-msvc',
}

const ESBUILD_NATIVE = {
	'darwin-arm64': '@esbuild/darwin-arm64',
	'darwin-x64': '@esbuild/darwin-x64',
	'linux-arm64': '@esbuild/linux-arm64',
	'linux-x64': '@esbuild/linux-x64',
	'win32-arm64': '@esbuild/win32-arm64',
	'win32-x64': '@esbuild/win32-x64',
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

const getRollupPackage = () => {
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
	const packagePath = join(root, 'node_modules', name)
	if (existsSync(packagePath)) {
		return
	}

	console.log(`Installing missing native binary: ${name}@${version}`)
	execSync(`npm install --no-save --no-package-lock ${name}@${version}`, {
		cwd: root,
		stdio: 'inherit',
	})
}

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

const rollupPackage = getRollupPackage()
if (rollupPackage) {
	ensurePackage(rollupPackage, rollupVersion)
}

const esbuildPackage = ESBUILD_NATIVE[`${platform}-${arch}`]
if (esbuildPackage) {
	ensurePackage(esbuildPackage, esbuildVersion)
	chmodEsbuild(esbuildPackage)
}