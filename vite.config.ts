import { defineConfig } from 'vitest/config'
import vue from '@vitejs/plugin-vue'
import vueJsx from '@vitejs/plugin-vue-jsx'
import path from 'path'

export default defineConfig({
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
		include: ['frontend/test/unit/**/*.{test,spec}.ts'],
	},
})
