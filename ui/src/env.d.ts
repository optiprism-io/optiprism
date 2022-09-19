/// <reference types="vite/client" />
import { defineComponent } from 'vue'

declare module '*.vue' {
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