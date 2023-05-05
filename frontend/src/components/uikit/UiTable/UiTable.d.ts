import {defineComponent} from 'vue'

export type StyleCell = {
    left?: string
    width?: string
    maxWidth?: string
    minWidth?: string
}

export type ToolMenuItem = {
    value: string | number
    label: string
}

export type Action = {
    name: string
    icon?: string
    type?: string | number
}

export type ColumnGroup = {
    title: string;
    value: string;
    span: number;
    lastFixed?: boolean;
    fixed?: boolean;
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
    fixed?: boolean
    enabled?: boolean
    default?: boolean
    type?: string
    notActiveStart?: boolean
    fitContent?: boolean
}

export type Cell = {
    key: string,
    value?: string | number | boolean | string[]
    title: string | number | boolean
    truncate?: boolean
    left?: number
    actions?: Action[]
    action?: Action,
    component?: ReturnType<typeof defineComponent>
    style?: StyleCell | undefined
    nowrap?: boolean
    lastFixed?: boolean
    fixed?: boolean
    type?: string
    items?: ToolMenuItem[]
}

export type ColumnMap = {
    [key: string]: Column;
}

export type Row = Cell[];
