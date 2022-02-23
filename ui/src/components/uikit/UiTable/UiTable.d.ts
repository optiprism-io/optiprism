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
    value: string
    title: string
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    left?: number
}

export type Row = Cell[];
