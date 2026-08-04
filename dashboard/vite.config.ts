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

import { defineConfig, Plugin } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";

const proxyHost = "http://localhost:21000";

let apacheLicense = "";
let apacheHtmlLicense = "";
try {
  const viteConfigContent = fs.readFileSync(path.resolve(__dirname, "vite.config.ts"), "utf-8");
  const extracted = viteConfigContent.substring(viteConfigContent.indexOf("/*"), viteConfigContent.indexOf("*/") + 2);
  if (extracted.includes("Licensed to the Apache Software Foundation")) {
    apacheLicense = extracted + "\n";
    apacheHtmlLicense = `\n<!--\n${extracted.replace('/*', '').replace('*/', '').trim()}\n-->\n`;
  } else {
    console.warn("Warning: Could not safely extract the Apache License from vite.config.ts!");
  }
} catch (e) {
  console.warn("Warning: Failed to read vite.config.ts for license extraction", e);
}

const addLicensePlugin = (): Plugin => {
  return {
    name: 'add-license',
    apply: 'build' as const, // only run during build
    generateBundle(_options: any, bundle: any) {
      if (!apacheLicense) return;
      for (const [fileName, chunk] of Object.entries(bundle)) {
        if (fileName.endsWith('.js') || fileName.endsWith('.css')) {
          if (chunk && typeof chunk === 'object') {
            if ('type' in chunk && chunk.type === 'chunk' && 'code' in chunk) {
              (chunk as any).code = apacheLicense + (chunk as any).code;
            } else if ('type' in chunk && chunk.type === 'asset' && 'source' in chunk && typeof (chunk as any).source === 'string') {
              (chunk as any).source = apacheLicense + (chunk as any).source;
            }
          }
        }
      }
    },
    transformIndexHtml(html: string) {
      if (!apacheHtmlLicense) return html;
      return html.replace('<head>', `<head>${apacheHtmlLicense}`);
    }
  };
};

export default defineConfig({
  plugins: [react(), addLicensePlugin()],
  base: "",
  build: {
    chunkSizeWarningLimit: 2000,
    reportCompressedSize: false,
    outDir: "dist/n3",
    rollupOptions: {
      input: "./index.html",
      output: {
        manualChunks: {
          react: ["react", "react-dom"],
          mui: [
            "@mui/material",
            "@mui/icons-material",
            "@emotion/react",
            "@emotion/styled"
          ],
          redux: ["redux", "@reduxjs/toolkit", "react-redux"],
          router: ["react-router-dom"],
          d3: ["d3", "d3-tip", "dagre-d3"],
          utils: ["moment-timezone"]
        }
      }
    }
  },
  optimizeDeps: {
    include: [
      "@mui/material",
      "@mui/icons-material",
      "@emotion/react",
      "@emotion/styled",
      "react-quill-new",
      "@mui/material/Tooltip"
    ]
  },
  server: {
    host: true,
    proxy: {
      "^/api/atlas/.*": {
        target: proxyHost,
        configure: (proxy, options) => {
          const username = "admin";
          const password = "admin";
          options.auth = `${username}:${password}`;
        }
      }
    }
  },
  resolve: {
    alias: {
      "@": `${path.resolve(__dirname, "./src")}`,
      "@components": `${path.resolve(__dirname, "./src/components")}`,
      // "@img": `${path.resolve(__dirname, "./src/img")}`,
      "@api": `${path.resolve(__dirname, "./src/api")}`,
      "@utils": `${path.resolve(__dirname, "./src/utils")}`,
      "@styles": `${path.resolve(__dirname, "./src/styles")}`,
      "@services": `${path.resolve(__dirname, "./src/services")}`,
      "@views": `${path.resolve(__dirname, "./src/views")}`,
      "@hooks": `${path.resolve(__dirname, "./src/hooks")}`,
      "@models": `${path.resolve(__dirname, "./src/models")}`,
      "@contexts": `${path.resolve(__dirname, "./src/contexts")}`,
      "@redux": `${path.resolve(__dirname, "./src/redux")}`
    }
  }
});
