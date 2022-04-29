/// <reference types="vite/client" />

import {DefineComponent} from 'vue';

declare module '*.vue' {
    import { defineComponent } from 'vue'
    const component: ReturnType<typeof defineComponent>

    export default component
}

declare module '@vue/runtime-core' {
    interface ComponentCustomProperties {
        $t: (key: string) => string
    }
}
declare module 'v-tooltip';
declare module 'VirtualisedList';