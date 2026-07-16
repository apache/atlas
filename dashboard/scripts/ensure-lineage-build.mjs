#!/usr/bin/env node
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
 * Opt-in via `npm run build:lineage`: build atlas-lineage webpack dist for legacy v2/v3.
 * Not used by default Vite build (React UI imports atlas-lineage/src directly).
 * Skips nested npm ci/webpack when dist is missing or sources changed.
 * Skips nested npm/webpack when an up-to-date dist is already present (e.g. copied by Maven).
 */
import { execSync } from 'node:child_process'
import { existsSync, readdirSync, statSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const dashboardRoot = join(scriptDir, '..')
const lineageDir = join(
	dashboardRoot,
	'src/views/Lineage/atlas-lineage'
)
const distIndex = join(lineageDir, 'dist/index.js')
const nodeModules = join(lineageDir, 'node_modules')

const IGNORE_DIRS = new Set(['node_modules', 'dist', '.cache'])

const walkMaxMtime = (dir) => {
	if (!existsSync(dir)) {
		return 0
	}

	let max = 0
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const fullPath = join(dir, entry.name)
		if (entry.isDirectory()) {
			if (IGNORE_DIRS.has(entry.name)) {
				continue
			}
			max = Math.max(max, walkMaxMtime(fullPath))
		} else if (entry.isFile()) {
			max = Math.max(max, statSync(fullPath).mtimeMs)
		}
	}
	return max
}

const getSourceMaxMtime = () => {
	const files = [
		join(lineageDir, 'package.json'),
		join(lineageDir, 'package-lock.json'),
		join(lineageDir, 'webpack.config.js'),
	]
	let max = walkMaxMtime(join(lineageDir, 'src'))
	for (const file of files) {
		if (existsSync(file)) {
			max = Math.max(max, statSync(file).mtimeMs)
		}
	}
	return max
}

const isDistUpToDate = () => {
	if (!existsSync(distIndex)) {
		return false
	}
	const distMtime = statSync(distIndex).mtimeMs
	return distMtime >= getSourceMaxMtime()
}

const run = (cmd) => {
	execSync(cmd, { cwd: lineageDir, stdio: 'inherit' })
}

if (process.env.ATLAS_FORCE_LINEAGE_BUILD === '1') {
	console.log('atlas-lineage: forced rebuild (ATLAS_FORCE_LINEAGE_BUILD=1)')
} else if (isDistUpToDate()) {
	console.log('atlas-lineage: dist is up to date, skipping nested npm/webpack')
	process.exit(0)
}

if (!existsSync(nodeModules)) {
	console.log('atlas-lineage: installing dependencies (npm ci)')
	run('npm ci --no-audit --no-fund')
}

console.log('atlas-lineage: building webpack bundle')
run('npm run build')
