import { defineComponent } from 'vue'

export type StyleCell = {
    left?: string
    width?: string
    maxWidth?: string
    minWidth?: string
}

export type Action = {
    name: string
    icon?: string
    type?: string | number
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
    style?: StyleCell
}

export type Cell = {
    value: string | number
    title: string | number
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    left?: number
    actions?: Action[]
    action?: Action,
    component?: ReturnType<typeof defineComponent>
    style?: StyleCell | undefined
}

export type EventCell = Cell & {
    customEvents: {
        name: string,
        value: number
    }[]
}

export type ColumnMap = {
    [key: string]: Column;
}

export type Row = Cell[];
