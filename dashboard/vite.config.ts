import { defineConfig, loadEnv } from "vite";
import preact from "@preact/preset-vite";
import { viteSingleFile } from "vite-plugin-singlefile";

// The whole dashboard ships as one HTML file so the server can `include_str!`
// it into the binary — no asset routes, no cache busting, no static dir.
export default defineConfig(({ mode }) => ({
  plugins: [preact(), viteSingleFile({ removeViteModuleLoader: true })],
  build: {
    target: "es2022",
    assetsInlineLimit: 100_000_000,
    cssCodeSplit: false,
    reportCompressedSize: false,
    chunkSizeWarningLimit: 100_000,
  },
  server: {
    // `bun run dev` talks to a node running locally.
    proxy: Object.fromEntries(
      ["/stats", "/sync", "/reports", "/archive", "/admin"].map((p) => [
        p,
        { target: loadEnv(mode, process.cwd(), "").NODE_URL || "http://127.0.0.1:8080", ws: true },
      ]),
    ),
  },
}));
