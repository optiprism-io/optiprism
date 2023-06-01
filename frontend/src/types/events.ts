import {OperationId} from '@/types'
import {Each} from '@/components/uikit/UiCalendar/UiCalendar'

import {
    DataType,
    Event,
    EventType,
    PropertyType,
    CustomEvent,
    Value,
    QuerySimpleTypeEnum,
    QueryCountPerGroupTypeEnum,
    DidEventAggregatePropertyTypeEnum,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryFormulaTypeEnum,
    QueryAggregatePerGroup,
    QueryAggregate,
    SegmentConditionAnd,
    SegmentConditionOr,
} from '@/api'

export type QueryType = QuerySimpleTypeEnum | QueryCountPerGroupTypeEnum | DidEventAggregatePropertyTypeEnum | QueryAggregatePropertyPerGroupTypeEnum | QueryFormulaTypeEnum;

export type PropertyRef = {
    type: PropertyType;
    id: number,
    name?: string,
};

export enum EventStatus {
    Enabled = 'enabled',
    Disabled = 'disabled'
}

export type EventRef = {
    type: EventType
    id: number
    name?: string
}

export function eventRef(e: Event): EventRef {
    return <EventRef>{ type: EventType.Regular, id: e.id, name: e.name }
}

export function customEventRef(e: CustomEvent): EventRef {
    return <EventRef>{ type: EventType.Custom, id: e.id, name: e.name }
}

export function eventPropertyRef(e: EventProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Event, id: e.id };
}

export function eventCustomPropertyRef(e: EventCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Custom, id: e.id };
}

export function userPropertyRef(e: UserProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.User, id: e.id };
}

export function userCustomPropertyRef(e: UserCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Custom, id: e.id };
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
    dataType: DataType;
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
    dataType: DataType;
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

export type EventQueryRef = {
    type?: QueryType;
    typeAggregate?: QueryAggregate;
    typeGroupAggregate?: QueryAggregatePerGroup;
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
        type: QuerySimpleTypeEnum.CountEvents,
        name: 'countEvents',
        displayName: 'Count',
    },
    {
        type: QuerySimpleTypeEnum.CountUniqueGroups,
        name: 'countUnique',
        displayName: 'Count Unique',
        grouped: true,
    },
    {
        type: QuerySimpleTypeEnum.WeeklyActiveGroups,
        name: 'dailyActive',
        displayName: 'Daily Active',
        grouped: true,
    },
    {
        type: QuerySimpleTypeEnum.WeeklyActiveGroups,
        name: 'weeklyActive',
        displayName: 'Weekly Active',
        grouped: true,
    },
    {
        type: QuerySimpleTypeEnum.MonthlyActiveGroups,
        name: 'monthlyActive',
        displayName: 'Monthly Active',
        grouped: true,
    },
    {
        type: QueryCountPerGroupTypeEnum.CountPerGroup,
        name: 'countPer',
        displayName: 'Count',
        grouped: true,
        hasAggregate: true,
    },
    {
        type: DidEventAggregatePropertyTypeEnum.AggregateProperty,
        name: 'aggregateProperty',
        displayName: 'Aggregate Property',
        hasAggregate: true,
        hasProperty: true
    },
    {
        type: QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup,
        name: 'aggregatePropertyPer',
        displayName: 'Aggregate Property per',
        grouped: true,
        hasAggregate: true,
        hasGroupAggregate: true,
        hasProperty: true
    },
    {
        type: QueryFormulaTypeEnum.Formula,
        name: 'formula',
        displayName: 'Formula',
        hasValue: true,
    },
]

export interface ConditionFilter {
    propRef?: PropertyRef
    opId: OperationId
    values: Value[]
    valuesList: Value[]
    error?: boolean
}

export interface Condition {
    action?: {
        name?: string
        id: string | SegmentConditionAnd | SegmentConditionOr
    }
    propRef?: PropertyRef
    opId?: OperationId
    values?: Value[]
    valueItem?: Value,
    valuesList?: Value[],
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