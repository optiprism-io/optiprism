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
    truncate?: boolean
    minWidth?: boolean
    maxWidth?: number | string
    width?: number | string
    left?: number
    style?: StyleCell
    lastFixed?: boolean
    fixed?: boolean,
}

export type Cell = {
    value: string | number | boolean
    title: string | number | boolean
    truncate?: boolean
    left?: number
    actions?: Action[]
    action?: Action,
    component?: ReturnType<typeof defineComponent>
    style?: StyleCell | undefined
    nowrap?: boolean
    lastFixed?: boolean
    fixed?: boolean,
}

export type ColumnMap = {
    [key: string]: Column;
}

export type Row = Cell[];
