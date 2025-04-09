import { defineConfig } from "vite";

export default defineConfig({
    build: {
        target: "esnext",   // target modern browsers
    },
    server: {
        port: 3000,         // serve on localhost:3000
        root: "public",     // serve from public folder
        open: true,
    },
});
