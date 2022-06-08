import UiCellTags from '@/components/uikit/cells/UiCellTegs.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import UiTextarea from '@/components/uikit/UiTextarea.vue'

export type Item = {
    key: string,
    type: 'label' | 'text' | 'input' | 'select'
    string: string
    component?: 'UiCellTags'
}

export default {
    'tags': {
        'key': 'tags',
        'string': 'events.event_management.popup.event_columns.tags',
        'component': UiCellTags
    },
    'name': {
        'key': 'name',
        'string': 'events.event_management.popup.event_columns.name'
    },
    'displayName': {
        'key': 'displayName',
        'string': 'events.event_management.popup.event_columns.displayName',
        'component': UiInput,
    },
    'description': {
        'key': 'description',
        'string': 'events.event_management.popup.event_columns.description',
        'component': UiTextarea,
    },
    'status': {
        'key': 'status',
        'string': 'events.event_management.popup.event_columns.status',
        'component': UiSwitch
    }
}