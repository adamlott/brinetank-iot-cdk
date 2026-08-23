import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  define: {
    // amazon-cognito-identity-js expects Node's `global` to exist
    global: "globalThis",
  },
});
