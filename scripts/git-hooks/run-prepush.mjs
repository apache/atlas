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
 * Pre-push: no checks (all dashboard verification runs on pre-commit).
 * Hook file kept so core.hooksPath stays stable; exits immediately.
 */

if (
	process.env.SKIP_ATLAS_HOOKS === '1' ||
	process.env.SKIP_ALL_ATLAS_GIT_HOOKS === '1'
) {
	process.exit(0)
}

process.exit(0)
