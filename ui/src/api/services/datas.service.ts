import { EventsApi, EventListRequest } from '@/api'

const api = new EventsApi()

const dataService = {
    createEventsStream: async(projectId: string, eventListRequest: EventListRequest) => await api.eventsStream(projectId, eventListRequest),
}

export default dataService
