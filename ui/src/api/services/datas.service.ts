import {EventsApi, EventListRequest, QueryApi} from '@/api'

const api = new EventsApi()
const queryApi = new QueryApi()

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventListRequest) => await api.eventsStream(organizationId, projectId, eventListRequest),
    funnelQuery: async () => await queryApi.funnelQuery()
}

export default dataService
