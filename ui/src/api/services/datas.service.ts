import {EventsApi, EventListRequest, QueryApi, FunnelQuery} from '@/api'

const api = new EventsApi()
const queryApi = new QueryApi()

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventListRequest) => await api.eventsStream(organizationId, projectId, eventListRequest),
    funnelQuery: async (organizationId: number, projectId: number, query: FunnelQuery) => await queryApi.funnelQuery(organizationId, projectId, query)
}

export default dataService
