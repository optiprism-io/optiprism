import { get } from '../apiClient'
import { Value, OperationId } from '@/types'
import { PropertyRef } from '@/types/events'
import { EventsApi, CreateCustomEventRequest, PropertyType, CustomEventEvent, EventType } from '@/api'

const api = new EventsApi()

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

export interface Event extends Omit<CustomEventEvent, 'eventType'> {
    eventType: EventType
}

export interface CustomEvents extends Omit<CreateCustomEventRequest, 'events'> {
    events: Array<Event>
}

const schemaService = {
    events: async () => await get('/schema/events', '', null),
    // updateEvent: async(projectId: string, eventId: string, params: Events) => await api.updateEvent(projectId, eventId, params),

    customEvents: async (projectId: string) => await api.customEventsList(projectId),
    createCustomEvent: async (projectId: string, params: CreateCustomEventRequest) => await api.createCustomEvent(projectId, params),
    updateCustomEvent: async(projectId: string, eventId: string, params: CreateCustomEventRequest) => await api.updateCustomEvent(projectId, eventId, params),

    eventProperties: async () => await get('/schema/event-properties', '', null),
    eventCustomProperties: async () => await get('/schema/event-custom-properties', '', null),

    userProperties: async () => await get('/schema/user-properties', '', null),
    userCustomProperties: async () => await get('/schema/user-custom-properties', '', null),

    propertyValues: async (params: PropertiesValues) => await get('/data/property-values', '', params),
};

export default schemaService;
