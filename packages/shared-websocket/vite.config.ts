import path from "path";
import { type PluginOption } from "vite";
import dts from "vite-plugin-dts";
import { defineConfig } from "vitest/config";

export default defineConfig({
  build: {
    minify: false,
    lib: {
      name: "SharedWebSocket",
      entry: path.resolve(__dirname, "src/index.ts"),
      formats: ["es", "cjs"],
      fileName: (format) =>
        `shared-websocket.${format === "es" ? "js" : "cjs"}`,
    },
    sourcemap: true,
  },
  plugins: [dts() as PluginOption],
});
