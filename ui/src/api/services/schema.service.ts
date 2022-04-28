import { get, fetch } from '../apiClient'
import { Value, OperationId } from '@/types'
import { EventRef, PropertyRef } from '@/types/events'
import { DefaultApi, CreateCustomEventRequest, PropertyType, CustomEventEvent } from '@/api'

const api = new DefaultApi()

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

export type Event = CustomEventEvent
export type CustomEvents = CreateCustomEventRequest

const schemaService = {
    events: async () => await get('/schema/events', '', null),

    customEvents: async (projectId: string) => await api.customEvents(projectId),
    createCustomEvent: async (projectId: string, params: CreateCustomEventRequest) => await api.createCustomEvent(projectId, params),
    updateCustomEvent: async(params: CustomEvents) => await fetch('/schema/custom-events', 'PUT', params),

    eventProperties: async () => await get('/schema/event-properties', '', null),
    eventCustomProperties: async () => await get('/schema/event-custom-properties', '', null),

    userProperties: async () => await get('/schema/user-properties', '', null),
    userCustomProperties: async () => await get('/schema/user-custom-properties', '', null),

    propertryValues: async (params: PropertiesValues) => await get('/data/property-values', '', params),
};

export default schemaService;
