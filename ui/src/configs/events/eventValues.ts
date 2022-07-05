import { defineComponent } from 'vue'

import UiInputTags from '@/components/uikit/UiInputTags.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import UiTextarea from '@/components/uikit/UiTextarea.vue'

export type Item = {
    key: string,
    type: 'label' | 'text' | 'input' | 'select'
    string: string
    editable?: boolean
    component?: ReturnType<typeof defineComponent>
}

export const eventValuesConfig = {
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
    },
    'tags': {
        'key': 'tags',
        'string': 'events.event_management.popup.event_columns.tags',
        'component': UiInputTags,
    }
}

export const propertyValuesConfig = {
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
    'type': {
        'key': 'type',
        'type': 'text',
        'string': 'events.event_management.popup.event_columns.type',
    },
    'status': {
        'key': 'status',
        'string': 'events.event_management.popup.event_columns.status',
        'component': UiSwitch
    },
    'tags': {
        'key': 'tags',
        'string': 'events.event_management.popup.event_columns.tags',
        'component': UiInputTags,
    },
}