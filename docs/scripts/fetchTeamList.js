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

const https = require("https");
const fs = require("fs");
const { parseString } = require("xml2js");

const POM_URL = "https://raw.githubusercontent.com/apache/atlas/master/pom.xml";
const OUTPUT_PATH = "src/resources/data/team.json";

function fetchXML(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, res => {
        if (res.statusCode !== 200) {
          reject(new Error(`Request failed. Status code: ${res.statusCode}`));
          res.resume(); // drain
          return;
        }

        let data = "";
        res.setEncoding("utf8");
        res.on("data", chunk => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

(async () => {
  try {
    const xmlData = await fetchXML(POM_URL);

    parseString(xmlData, (err, result) => {
      if (err) {
        console.error("❌ XML parsing failed:", err);
        process.exit(1);
      }

      let developersList = [];
      if (
        result &&
        result.project &&
        result.project.developers &&
        Array.isArray(result.project.developers) &&
        result.project.developers[0] &&
        result.project.developers[0].developer
      ) {
        developersList = result.project.developers[0].developer;
      }

      const keys = developersList.length > 0 ? Object.keys(developersList[0]) : [];

      const output = developersList.map(dev => {
        const obj = {};
        keys.forEach(k => {
          obj[k] = dev[k] || [""];
        });
        return obj;
      });

      // Ensure the directory exists
      const outputDir = require("path").dirname(OUTPUT_PATH);
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));
      console.log(`✅ Team data written to ${OUTPUT_PATH}`);
    });
  } catch (err) {
    console.error("❌ Failed to fetch pom.xml:", err.message);
    process.exit(1);
  }
})();