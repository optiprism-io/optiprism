# Vue 3 + Typescript + Vite

`node v16.11.1 (npm v8.0.0)`

Startup and installation using standard commands from the vite (scripts in package.json)


## Info
- Сoding style is based on rules from `.eslintrc.js`
- Сheck with command before deployment: `$ npm run lint` and `$ npm run build`
- For styles and html we use [Patternfly](https://www.patternfly.org/v4/) and [FloatingVue](https://github.com/Akryum/floating-vue) for floating elements such as popups, tooltips, dropdowns etc.

## Structure
 - **components** -
    - common - for common components like - header, footer and other
    - uikit - application components built on [Patternfly](https://www.patternfly.org/v4/)
    - others folders are components from the sections of the application
 - **helpers** - helper functions that will be used throughout the application
 - **i18n** - plugin for translations, translations(JSON-s) are in the **langs** folder. Is used through the $t operator and `inject('i18n')` in setup-like components
 - **api** - api generated from the openapi file (search above for it - **openapi.yaml**). To use structure types and methods. API works in conjunction with Axios. Access to api through services (wrapper over methods from open api). To generate api you need to use - [openapi-generator](https://openapi-generator.tech/docs/generators/typescript-axios/), install it on the local machine and run it with the command - `$ openapi-generator generate -i ../api/openapi.yaml -g typescript-axios -o ./src/api`
 - **configs** - data for structuring components, initial data
 - **server.ts** - stub for api, for local work without api at the moment we use this file. It takes [miragejs](https://miragejs.com/) and data from the mocks folder and makes it possible to work with requests
 - **types** - the folder contains common types for the entire application; types can also be located next to components. Core types for structures is taken and api(/api/api) generated from OpenApi
 - **stores** - [Pinia](https://pinia.vuejs.org/) is used to organize and store data. Develop from the logic of the task, by sections, store common data used throughout the application in common storage



This template should help get you started developing with Vue 3 and Typescript in Vite. The template uses Vue 3 `<script setup>` SFCs, check out the [script setup docs](https://v3.vuejs.org/api/sfc-script-setup.html#sfc-script-setup) to learn more.

## Recommended IDE Setup

-   [VSCode](https://code.visualstudio.com/) + [Volar](https://marketplace.visualstudio.com/items?itemName=johnsoncodehk.volar)

## Type Support For `.vue` Imports in TS

Since TypeScript cannot handle type information for `.vue` imports, they are shimmed to be a generic Vue component type by default. In most cases this is fine if you don't really care about component prop types outside of templates. However, if you wish to get actual prop types in `.vue` imports (for example to get props validation when using manual `h(...)` calls), you can enable Volar's `.vue` type support plugin by running `Volar: Switch TS Plugin on/off` from VSCode command palette.

### before push:

```
$ npm run lint
$ npm run build
```

### For generate API client

- [Doc](https://openapi-generator.tech/docs/generators/typescript-axios/)
- [Install Doc](https://openapi-generator.tech/docs/installation/) `(min version 6.0.1)`

```
openapi-generator generate -i ../api/openapi.yaml -g typescript-axios -o ./src/api
```