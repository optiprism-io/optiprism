import {defineComponent} from 'vue'
import EventCell from '@/components/events/EventCell.vue'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'

export const componentsMaps: { [n: string]: ReturnType<typeof defineComponent> | null } = {
    'eventName': UiTablePressedCell,
    'customEvents': EventCell,
    'createdAt': null,
}