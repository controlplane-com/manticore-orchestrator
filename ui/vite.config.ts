import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: parseInt(process.env.PORT || '5173'),
    proxy: {
      '/api': {
        target: process.env.API_URL || 'http://localhost:8080',
        changeOrigin: true,
        headers: process.env.ORCHESTRATOR_API_TOKEN
          ? { Authorization: `Bearer ${process.env.ORCHESTRATOR_API_TOKEN}` }
          : {},
      }
    }
  },
  build: {
    outDir: 'dist',
  }
})
