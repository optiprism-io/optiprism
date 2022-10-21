export interface Action {
    type: string
    link?: string
    icon?: string
    text: string
}

export interface Item<T, K = undefined>{
    item: T;
    name: string;
    description?: string;
    disabled?: boolean | undefined;
    items?: K | undefined;
    editable?: boolean
    selected?: boolean
}

export interface Group<T>{
    type?: string
    name: string
    items: T
    action?: Action
}
