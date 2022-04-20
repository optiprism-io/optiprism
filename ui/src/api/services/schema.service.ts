import { get, fetch } from '../apiClient'
import { Value, OperationId } from '../../types'
import { EventRef, PropertyRef } from '@/types/events'
import { CreateCustomEventRequest, PropertyType } from '@/api'

type PropertiesValues = {
    event_name?: string;
    event_type?: string;
    property_name: string;
    property_type?: string;
};

export type FilterCustomEvent = {
    filterType: string
    propertyName: string
    propertyType: PropertyType,
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

export type CustomEvents = Omit<CreateCustomEventRequest, 'events'> & {
    id?: number // TODO integration
    events?: Event[]
};

const schemaService = {
    events: async () => await get("/schema/events", "", null),

    customEvents: async () => await get("/schema/custom-events", "", null),
    createCustomEvent: async(params: CustomEvents) => await fetch('/schema/custom-events', 'POST', params),
    editCustomEvent: async(params: CustomEvents) => await fetch('/schema/custom-events', 'PUT', params),

    eventProperties: async () => await get("/schema/event-properties", "", null),
    eventCustomProperties: async () => await get("/schema/event-custom-properties", "", null),

    userProperties: async () => await get("/schema/user-properties", "", null),
    userCustomProperties: async () => await get("/schema/user-custom-properties", "", null),

    propertryValues: async (params: PropertiesValues) => await get("/data/property-values", "", params),
};

export default schemaService;
