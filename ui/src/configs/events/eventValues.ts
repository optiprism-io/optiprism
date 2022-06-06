export type Item = {
    key: string,
    type: 'label' | 'text' | 'input' | 'select'
    string: string
}

export default {
    'tags': {
        'key': 'tags',
        'type': 'label',
        'string': 'events.event_management.popup.event_columns.tags'
    },
    'name': {
        'key': 'name',
        'type': 'string',
        'string': 'events.event_management.popup.event_columns.name'
    },
    'displayName': {
        'key': 'displayName',
        'type': 'input',
        'string': 'events.event_management.popup.event_columns.displayName'
    },
    'description': {
        'key': 'description',
        'type': 'string',
        'string': 'events.event_management.popup.event_columns.description'
    },
    'status': {
        'key': 'status',
        'type': 'select',
        'string': 'events.event_management.popup.event_columns.status'
    }
}