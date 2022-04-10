import { get, fetch } from '../apiClient'
import { Value, OperationId } from '../../types'
import { EventRef, PropertyRef } from '@/types/events'

type PropertiesValues = {
    event_name?: string;
    event_type?: string;
    property_name: string;
    property_type?: string;
};

export type FilterCustomEvent = {
    filterType: string
    propertyName: string
    propertyType: string
    operation: OperationId
    value: Value[]
    propRef: PropertyRef
    valuesList?: string[]
}

export type Event = {
    eventName: string
    eventType: string
    filters?: FilterCustomEvent[]
    ref: EventRef
}

export type CustomEvents = {
    // projectId: number, TODO integration
    name: string,
    id?: number,
    events: Event[]
}

const schemaService = {
    events: async () => await get("/schema/events", "", null),

    customEvents: async () => await get("/schema/custom-events", "", null),
    createCustomEvents: async(params: CustomEvents) => await fetch('/schema/custom-events', 'POST', params),
    editCustomEvents: async(params: CustomEvents) => await fetch('/schema/custom-events', 'PUT', params),

    eventProperties: async () => await get("/schema/event-properties", "", null),
    eventCustomProperties: async () => await get("/schema/event-custom-properties", "", null),

    userProperties: async () => await get("/schema/user-properties", "", null),
    userCustomProperties: async () => await get("/schema/user-custom-properties", "", null),

    propertiesValues: async (params: PropertiesValues) => await get("/data/property-values", "", params),

    getEventChart: async () => await get("/chart", "", null),
};

export default schemaService;
