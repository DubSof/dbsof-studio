import {defineConfig} from "vite";
import react from "@vitejs/plugin-react-swc";
import genericNames from "generic-names";
import path from "path";
import {fileURLToPath} from "url";

const __dirname =
  typeof __dirname !== "undefined"
    ? __dirname
    : path.dirname(fileURLToPath(import.meta.url));

// https://vite.dev/config/
export default defineConfig({
  plugins: [react({tsDecorators: true})],
  base: "/ui",
  build: {outDir: "build"},
  resolve: {
    alias: {
      "@dbsof/auth": path.resolve(__dirname, "shared/auth"),
    },
  },
  optimizeDeps: {
    entries: "./index.html",
  },
  esbuild: {
    // don't minify class names, to preserve error names from the backend
    keepNames: true,
  },
  css: {
    preprocessorOptions: {
      scss: {
        api: "modern-compiler",
      },
    },
    modules: {
      generateScopedName(name, filename) {
        return genericNames("[name]_[local]__[hash:base64:5]")(
          name,
          filename
        ).replace("-module_", "_");
      },
    },
  },
  preview: {port: 3002},
  server: {port: 3002},
});
