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
 * Point this Git repo at .githooks (repo root) so pre-commit / pre-push run for
 * dashboard, dashboardv2, and docs. Runs after `npm install` in dashboard/.
 * Safe no-op if not inside a Git work tree.
 */

import { execSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = fileURLToPath(new URL('.', import.meta.url))
const dashboardDir = join(__dirname, '..')

let top
try {
	top = execSync('git rev-parse --show-toplevel', {
		encoding: 'utf8',
		cwd: dashboardDir,
	}).trim()
} catch {
	process.exit(0)
}

const hooksPath = '.githooks'
const absHooks = join(top, hooksPath)
if (!existsSync(absHooks)) {
	console.warn('[install-git-hooks] Skipping: missing', absHooks)
	process.exit(0)
}

try {
	execSync(`git config core.hooksPath "${hooksPath}"`, {
		cwd: top,
		stdio: 'inherit',
	})
	console.log('[install-git-hooks] core.hooksPath =', hooksPath)
} catch (e) {
	console.warn('[install-git-hooks] Could not set core.hooksPath (read-only?)')
}
