import { DataType } from '@/types';
import { AggregateId } from '@/types/aggregate';

export enum EventType {
    Regular,
    Custom
}

export enum PropertyType {
    Event,
    EventCustom,
    User,
    UserCustom
}

export type PropertyRef = {
    type: PropertyType;
    id: number;
};

export enum EventStatus {
    Enabled = "enabled",
    Disabled = "disabled"
}

export type EventRef = {
    type: EventType;
    id: number;
};

export function eventRef(e: Event): EventRef {
    return <EventRef>{ type: EventType.Regular, id: e.id };
}

export function customEventRef(e: CustomEvent): EventRef {
    return <EventRef>{ type: EventType.Custom, id: e.id };
}

export function eventPropertyRef(e: EventProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.Event, id: e.id };
}

export function eventCustomPropertyRef(e: EventCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.EventCustom, id: e.id };
}

export function userPropertyRef(e: UserProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.User, id: e.id };
}

export function userCustomPropertyRef(e: UserCustomProperty): PropertyRef {
    return <PropertyRef>{ type: PropertyType.UserCustom, id: e.id };
}

export interface CustomEvent {
    id: number;
    createdAt: Date;
    updatedAt?: Date;
    createdBy: number;
    updatedBy: number;
    projectId: number;
    tags: string[];
    name: string;
    description?: string;
    status: EventStatus;
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

export type QueryType = "simple" | "countPerGroup" | "aggregateProperty" | "aggregatePropertyPerGroup" | "formula";

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
        type: "simple",
        name: "countEvents",
        displayName: "Count",
    },
    {
        type: "simple",
        name: "countUnique",
        displayName: "Count Unique",
        grouped: true,
    },
    {
        type: "simple",
        name: "dailyActive",
        displayName: "Daily Active",
        grouped: true,
    },
    {
        type: "simple",
        name: "weeklyActive",
        displayName: "Weekly Active",
        grouped: true,
    },
    {
        type: "simple",
        name: "monthlyActive",
        displayName: "Monthly Active",
        grouped: true,
    },
    {
        type: "countPerGroup",
        name: "countPer",
        displayName: "Count",
        grouped: true,
        hasAggregate: true,
    },
    {
        type: "aggregateProperty",
        name: "aggregateProperty",
        displayName: "Aggregate Property",
        hasAggregate: true,
        hasProperty: true
    },
    {
        type: "aggregatePropertyPerGroup",
        name: "aggregatePropertyPer",
        displayName: "Aggregate Property per",
        grouped: true,
        hasAggregate: true,
        hasGroupAggregate: true,
        hasProperty: true
    },
    {
        type: "formula",
        name: "formula",
        displayName: "Formula",
        hasValue: true,
    },
]