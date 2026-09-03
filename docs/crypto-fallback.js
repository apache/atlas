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

const crypto = require("crypto");
const originalCreateHash = crypto.createHash;

crypto.createHash = (algorithm, options) => {
  if (algorithm === "md4") {
    try {
      return originalCreateHash("md4", options);
    } catch (e) {
      return originalCreateHash("sha256", options);
    }
  }
  return originalCreateHash(algorithm, options);
};

process.on("uncaughtException", function (err) {
  if (err && err.code === "ECONNRESET") {
    console.warn("Handled Docz build process ECONNRESET:", err.message);
    return;
  }
  throw err;
});

