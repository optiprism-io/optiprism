import { inject } from 'vue'
import { $T } from '@/utils/i18n'

export default function useI18n() {
    const t = inject('$t') as $T
    const keyExists = inject('$tkeyExists')

    return {
        t,
        keyExists,
    }
}