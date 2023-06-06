import { inject } from 'vue'
import { $T, $TKeyExists } from '@/utils/i18n'

export default function useI18n() {
    const t = inject('$t') as $T;
    const keyExists = inject('$tkeyExists') as $TKeyExists;

    return {
        t,
        keyExists,
    }
}