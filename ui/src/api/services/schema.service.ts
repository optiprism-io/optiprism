import {OperationId, Value} from '@/types'
import {PropertyRef} from '@/types/events'
import {
    CreateCustomEventRequest,
    CustomEventEvent,
    EventsApi,
    EventType,
    PropertiesApi,
    PropertyType,
    UpdateEventRequest,
    UpdatePropertyRequest,
    UpdateCustomEventRequest,
    PropertyValuesListRequest,
} from '@/api'

const api = new EventsApi()
const propertiesApi = new PropertiesApi()

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
    events: async (organizationId: number, projectId: number) => await api.eventsList(organizationId, projectId),
    updateEvent: async(organizationId: number, projectId: number, eventId: string, params: UpdateEventRequest) => await api.updateEvent(organizationId, projectId, eventId, params),

    customEvents: async (organizationId: number, projectId: number) => await api.customEventsList(organizationId, projectId),
    createCustomEvent: async (organizationId: number, projectId: number, params: CreateCustomEventRequest) => await api.createCustomEvent(organizationId, projectId, params),
    updateCustomEvent: async(organizationId: number, projectId: number, eventId: string, params: UpdateCustomEventRequest) => await api.updateCustomEvent(organizationId, projectId, eventId, params),
    deleteCustomEvents: async (organizationId: number, projectId: number, eventId: number) => await api.deleteCustomEvent(organizationId, projectId, eventId),

    eventProperties: async (organizationId: number, projectId: number) => await propertiesApi.eventPropertiesList(organizationId, projectId),
    updateEventProperty: async(organizationId: number, projectId: number, propertyId: string, params: UpdatePropertyRequest) => await propertiesApi.updateEventProperty(organizationId, projectId, propertyId, params),
    eventCustomProperties: async (organizationId: number, projectId: number) => await propertiesApi.customPropertiesList(organizationId, projectId),
    userProperties: async (organizationId: number, projectId: number) => await propertiesApi.userPropertiesList(organizationId, projectId),
    updateUserProperty: async(organizationId: number, projectId: number, propertyId: number, params: UpdatePropertyRequest) => await propertiesApi.updateUserProperty(organizationId, projectId, propertyId, params),

    propertyValues: async(organizationId: number, projectId: number, params: PropertyValuesListRequest) => await propertiesApi.propertyValuesList(organizationId, projectId, params),
};

export default schemaService;
