import { DefaultApi, EventListRequest } from '@/api'

const api = new DefaultApi()

const dataService = {
    createEventsStream: async(projectId: string, eventListRequest: EventListRequest) => await api.createEventsStream(projectId, eventListRequest),
}

export default dataService
