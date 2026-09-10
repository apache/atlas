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

const { spawnSync } = require('child_process');
const path = require('path');

const command = process.argv[2]; // 'dev' or 'build'
if (!command) {
  console.error("Please specify a command (dev or build)");
  process.exit(1);
}

const cryptoFallbackPath = path.join(__dirname, '..', 'crypto-fallback.js');
const doczBinPath = path.join(__dirname, '..', 'docz-lib', 'docz', 'bin', 'index.js');

const result = spawnSync('node', [doczBinPath, command], {
  stdio: 'inherit',
  env: {
    ...process.env,
    NODE_OPTIONS: `${process.env.NODE_OPTIONS || ''} --require ${cryptoFallbackPath}`.trim()
  }
});

process.exit(result.status || 0);
