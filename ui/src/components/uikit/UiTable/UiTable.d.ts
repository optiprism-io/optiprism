export type Column = {
    value: string
    title: string
    sorted?: boolean
    sort?: boolean
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
    minWidth?: boolean
}

export type Cell = {
    value: string
    title: string
    pinned?: boolean
    truncate?: boolean
    lastPinned?: boolean
}

export type Row = Cell[];
