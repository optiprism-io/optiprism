import { defineComponent } from 'vue'

export type Action = {
    name: string
    icon: string
}

export type Column = {
    value: string
    title: string
    sorted?: boolean
    sort?: boolean
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    minWidth?: boolean
    maxWidth?: number | string
    width?: number | string
    left?: number
}

export type Cell = {
    value: string | number
    title: string | number
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    left?: number
    actions?: Action[]
    component?: ReturnType<typeof defineComponent>
}

export type EventCell = Cell & {
    customEvents: string[]
}

export type ColumnMap = {
    [key: string]: Column;
}

export type Row = Cell[];
