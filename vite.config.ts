import { defineConfig } from 'vitest/config'
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
			'@': path.resolve('frontend', 'src')
		}
	},
	test: {
		include: ['test/unit/**/*.{test,spec}.ts'],
	},
})
