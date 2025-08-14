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

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";

const proxyHost = "http://localhost:21000";

export default defineConfig({
  plugins: [react()],
  base: "",
  build: {
    chunkSizeWarningLimit: 1000,
    outDir: "dist/n3",
    rollupOptions: {
      input: "./index.html",
      output: {
        manualChunks: {
          react: ["react", "react-dom"]
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
