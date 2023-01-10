import {OperationId, Value} from '@/types'
import {PropertyRef} from '@/types/events'
import {
    CreateCustomEventRequest,
    CustomEventEvent,
    EventsApi,
    EventType,
    EventPropertiesApi,
    CustomEventsApi,
    PropertiesApi,
    PropertyValuesApi,
    UserPropertiesApi,
    PropertyType,
    UpdateEventRequest,
    UpdatePropertyRequest,
    UpdateCustomEventRequest,
    ListPropertyValuesRequest,
} from '@/api'
import {config} from '@/api/services/config';

const api = new EventsApi(config)
const customEventsApi = new CustomEventsApi(config)
const propertiesApi = new PropertiesApi(config)
const propertyValuesApi = new PropertyValuesApi(config)
const eventPropertiesApi = new EventPropertiesApi(config)
const userPropertiesApi = new UserPropertiesApi(config)

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
    customEvents: async (organizationId: number, projectId: number) => await customEventsApi.customEventsList(organizationId, projectId),
    createCustomEvent: async (organizationId: number, projectId: number, params: CreateCustomEventRequest) => await customEventsApi.createCustomEvent(organizationId, projectId, params),
    updateCustomEvent: async(organizationId: number, projectId: number, eventId: string, params: UpdateCustomEventRequest) => await customEventsApi.updateCustomEvent(organizationId, projectId, eventId, params),
    deleteCustomEvents: async (organizationId: number, projectId: number, eventId: number) => await customEventsApi.deleteCustomEvent(organizationId, projectId, eventId),
    eventCustomProperties: async (organizationId: number, projectId: number) => await propertiesApi.customPropertiesList(organizationId, projectId),
    eventProperties: async (organizationId: number, projectId: number) => await eventPropertiesApi.eventPropertiesList(organizationId, projectId),
    updateEventProperty: async(organizationId: number, projectId: number, propertyId: string, params: UpdatePropertyRequest) => await eventPropertiesApi.updateEventProperty(organizationId, projectId, propertyId, params),
    userProperties: async (organizationId: number, projectId: number) => await userPropertiesApi.userPropertiesList(organizationId, projectId),
    updateUserProperty: async(organizationId: number, projectId: number, propertyId: number, params: UpdatePropertyRequest) => await userPropertiesApi.updateUserProperty(organizationId, projectId, propertyId, params),
    propertyValues: async(organizationId: number, projectId: number, params: ListPropertyValuesRequest) => await propertyValuesApi.propertyValuesList(organizationId, projectId, params),
};

export default schemaService
