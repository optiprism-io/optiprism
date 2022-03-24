export type Each = 'month' | 'week' | 'day' | 'hour' | 'minute'

export interface RangeValue {
    i?: number;
    id: string;
    month?: number;
    year?: number;
}

export interface CurrentValue {
    from: null | string,
    to: null | string,
    dates: string[],
    multiple: boolean,
    type: string,
    activeDates: string[],
    date: string | null,
}

export interface ApplyPayload {
    value: CurrentValue,
    type: string,
    last: number,
}

export interface Value {
    from: string
    to: string
    multiple: boolean
    dates?: string[]
    each?: Each
}

export interface Ranged {
    from: string | null;
    to: string | null;
}