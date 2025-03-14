import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";

// const proxyHost = "http://ccycloud-3.cb719.root.comops.site:31000";
const proxyHost = "http://localhost:21000";

export default defineConfig({
  plugins: [react()],
  base: "",
  build: {
    outDir: "dist/n3", // Build output directory
    rollupOptions: {
      input: "./index.html" // Entry file for your React app
    }
  },
  optimizeDeps: {
    include: ["@emotion/react", "@emotion/styled", "@mui/material/Tooltip"]
  },
  server: {
    host: true,
    proxy: {
      "^/api/atlas/.*": {
        target: proxyHost,
        configure: (proxy, options) => {
          const username = "admin";
          const password = "admin123";
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
