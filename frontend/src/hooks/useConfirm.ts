import { inject } from 'vue'
import { Props } from '@/components/uikit/UiPopupWindow.vue'

export default function useConfirm() {
    const confirm = inject('$confirm') as (text: string, props: Props) => boolean

    return {
        confirm
    }
}