import { OperationId } from '@/types'
import { AggregateId } from '@/types/aggregate'
import { Each } from '@/components/uikit/UiCalendar/UiCalendar'
import { EventType, PropertyType, DataType, Value, CustomEvent } from '@/api'

export type PropertyRef = {
    type: PropertyType;
    name: string
};

export enum EventStatus {
    Enabled = 'enabled',
    Disabled = 'disabled'
}

export type EventRef = {
    type: EventType;
    name: string
}

export function eventRef(e: Event): EventRef {
    return <EventRef>{ type: EventType.Regular, name: e.name }
}

export function customEventRef(e: CustomEvent): EventRef {
    return <EventRef>{ type: EventType.Custom, name: e.name }
}

export function eventPropertyRef(e: EventProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Event, name: e.name };
}

export function eventCustomPropertyRef(e: EventCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Custom, name: e.name };
}

export function userPropertyRef(e: UserProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.User, name: e.name };
}

export function userCustomPropertyRef(e: UserCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Custom, name: e.name };
}

export interface Event {
    id: number;
    createdAt: Date;
    updatedAt?: Date;
    createdBy: number;
    updatedBy: number;
    projectId: number;
    isSystyem: boolean;
    tags: string[];
    name: string;
    displayName: string;
    description?: string;
    status: EventStatus;
    properties?: number[];
    custom_properties?: number[];
}

export interface EventProperty {
    id: number;
    createdAt: Date;
    updatedAt?: Date;
    createdBy: number;
    updatedBy: number;
    projectId: number;
    events: number[];
    isSystem: boolean;
    isGlobal: boolean;
    tags: string[];
    name: string;
    displayName: string;
    description: string;
    status: EventStatus;
    type: DataType;
    db_col: any;
    isRequired: boolean;
    nullable: boolean;
    isArray: boolean;
    isDictionary: boolean;
    dictionaryType?: DataType;
}

export interface EventCustomProperty {
    id: number;
    createdAt: Date;
    updatedAt?: Date;
    createdBy: number;
    updatedBy: number;
    projectId: number;
    events: number[];
    isSystem: boolean;
    status: EventStatus;
    name: string;
    description: string;
    type: DataType;
    nullable: boolean;
    isArray: boolean;
    tags: string[];
}

export interface UserProperty {
    id: number;
    createdBy: number;
    createdAt: Date;
    updatedAt?: Date;
    updatedBy: number;
    projectId: number;
    isSystem: boolean;
    tags: string[];
    name: string;
    displayName: string;
    description: string;
    status: EventStatus;
    type: DataType;
    db_col?: any;
    nullable: boolean;
    isArray: boolean;
    isDictionary: boolean;
    dictionaryType?: DataType;
}

export interface UserCustomProperty {
    id: number;
    createdBy: number;
    createdAt: Date;
    updatedAt?: Date;
    updatedBy: number;
    projectId: number;
    events: number[];
    isSystem: boolean;
    isGlobal: boolean;
    tags: string[];
    name: string;
    displayName: string;
    description: string;
    status: EventStatus;
    type: DataType;
    db_col?: any;
    isRequired: boolean;
    nullable: boolean;
    isArray: boolean;
    isDictionary: boolean;
    dictionaryType?: DataType;
}

export type QueryType = 'simple' | 'countPerGroup' | 'aggregateProperty' | 'aggregatePropertyPerGroup' | 'formula';

export type EventQueryRef = {
    type?: QueryType;
    typeAggregate?: AggregateId;
    typeGroupAggregate?: AggregateId;
    propRef?: PropertyRef;
    name?: string;
    value?: string;
};

export interface EventsQuery {
    type: QueryType;
    name?: string;
    displayName: string;
    hasAggregate?: boolean;
    grouped?: boolean;
    hasProperty?: boolean;
    hasGroupAggregate?: boolean;
    hasValue?: boolean;
}

export const eventsQueries: EventsQuery[] = [
    {
        type: 'simple',
        name: 'countEvents',
        displayName: 'Count',
    },
    {
        type: 'simple',
        name: 'countUnique',
        displayName: 'Count Unique',
        grouped: true,
    },
    {
        type: 'simple',
        name: 'dailyActive',
        displayName: 'Daily Active',
        grouped: true,
    },
    {
        type: 'simple',
        name: 'weeklyActive',
        displayName: 'Weekly Active',
        grouped: true,
    },
    {
        type: 'simple',
        name: 'monthlyActive',
        displayName: 'Monthly Active',
        grouped: true,
    },
    {
        type: 'countPerGroup',
        name: 'countPer',
        displayName: 'Count',
        grouped: true,
        hasAggregate: true,
    },
    {
        type: 'aggregateProperty',
        name: 'aggregateProperty',
        displayName: 'Aggregate Property',
        hasAggregate: true,
        hasProperty: true
    },
    {
        type: 'aggregatePropertyPerGroup',
        name: 'aggregatePropertyPer',
        displayName: 'Aggregate Property per',
        grouped: true,
        hasAggregate: true,
        hasGroupAggregate: true,
        hasProperty: true
    },
    {
        type: 'formula',
        name: 'formula',
        displayName: 'Formula',
        hasValue: true,
    },
]

export interface ConditionFilter {
    propRef?: PropertyRef
    opId: OperationId
    values: Value[]
    valuesList: string[]
    error?: boolean
}

export interface Condition {
    action?: {
        name?: string
        id: string
    }
    propRef?: PropertyRef
    opId?: OperationId
    values?: Value[]
    valueItem?: string | number
    valuesList?: string[]
    period?: {
        from?: string
        to?: string
        last?: number
        type?: string
    }
    event?: {
        name: string
        ref: EventRef
    }
    compareEvent?: {
        name: string
        ref: EventRef
    }
    filters: ConditionFilter[]
    aggregate?: {
        name?: string
        id: string,
        typeAggregate?: string
    }
    each?: Each
}