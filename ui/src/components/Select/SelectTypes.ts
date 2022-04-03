export interface Action {
    type: string
    link?: string
    icon?: string
    text: string
}

export interface Item<T, K>{
    item: T;
    name: string;
    description?: string;
    disabled?: boolean | undefined;
    items?: K | undefined;
}

export interface Group<T>{
    type?: string
    name: string
    items: T
    action?: Action
}
