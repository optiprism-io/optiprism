/// <reference types="vite/client" />
import { defineComponent } from 'vue'
import { $T } from '@/utils/i18n'

declare module '*.vue' {
    const component: ReturnType<typeof defineComponent>
    export default component
}

declare module '@vue/runtime-core' {
    interface ComponentCustomProperties {
        $t: $T
    }
}
declare module 'v-tooltip';
declare module 'VirtualisedList';