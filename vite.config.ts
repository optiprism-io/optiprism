import {defineConfig} from 'vitest/config'
import vue from '@vitejs/plugin-vue'
import vueJsx from '@vitejs/plugin-vue-jsx'
import path from 'path'

export default defineConfig({
    root: 'frontend',
    plugins: [
        vue(),
        vueJsx(),
    ],
    resolve: {
        alias: {
            '@': path.resolve(__dirname, 'frontend', 'src')
        }
    },
    test: {
        root: 'frontend'
    }
})
