import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      '/mapping': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      },
      '/metrics': {
        target: 'http://localhost:3000',
        changeOrigin: true,
      }
    }
  }
})
