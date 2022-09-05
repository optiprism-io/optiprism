import {get} from '../apiClient'
import {OperationId, Value} from '@/types'
import {PropertyRef} from '@/types/events'
import {
    CreateCustomEventRequest,
    CustomEventEvent,
    EventsApi,
    EventType,
    PropertiesApi,
    PropertyType,
    UpdateCustomEventRequest,
    UpdateEventRequest,
    UpdatePropertyRequest,
} from '@/api'

const api = new EventsApi()
const propertiesApi = new PropertiesApi()

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
    updateEvent: async(organizationId: number, projectId: number, eventId: string, params: UpdateEventRequest) => await api.updateEvent(organizationId, projectId, eventId, params),

    customEvents: async (organizationId: number, projectId: number) => await api.customEventsList(organizationId, projectId),
    createCustomEvent: async (organizationId: number, projectId: number, params: CreateCustomEventRequest) => await api.createCustomEvent(organizationId, projectId, params),
    updateCustomEvent: async(organizationId: number, projectId: number, eventId: string, params: UpdateCustomEventRequest) => await api.updateCustomEvent(organizationId, projectId, eventId, params),
    deleteCustomEvents: async (organizationId: number, projectId: number, eventId: number) => await api.deleteCustomEvent(organizationId, projectId, eventId),

    eventProperties: async () => await get('/schema/event-properties', '', null),
    updateEventProperty: async(organizationId: number, projectId: number, propertyId: string, params: UpdatePropertyRequest) => await propertiesApi.updateEventProperty(organizationId, projectId, propertyId, params),
    eventCustomProperties: async () => await get('/schema/event-custom-properties', '', null),

    userProperties: async () => await get('/schema/user-properties', '', null),
    updateUserProperty: async(organizationId: number, projectId: number, propertyId: number, params: UpdatePropertyRequest) => await propertiesApi.updateUserProperty(organizationId, projectId, propertyId, params),
    userCustomProperties: async () => await get('/schema/user-custom-properties', '', null),

    propertyValues: async (params: PropertiesValues) => await get('/data/property-values', '', params),
};

export default schemaService;
