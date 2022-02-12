export interface Item<T, K>{
    item: T;
    name: string;
    description?: string;
    disabled?: boolean | undefined;
    items?: K | undefined;
}

export interface Group<T>{
    name: string;
    items: T;
}
