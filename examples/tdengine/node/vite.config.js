import { defineConfig } from "vite";

export default defineConfig({
    build: {
        target: "esnext",
    },
    server: {
        port: 3000,
    },
});
