import { EventsApi, EventListRequest } from '@/api'

const api = new EventsApi()

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventListRequest) => await api.eventsStream(organizationId, projectId, eventListRequest),
}

export default dataService
