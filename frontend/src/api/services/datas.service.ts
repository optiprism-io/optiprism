import {EventRecordsListRequest, EventsApi, FunnelQuery, QueryApi} from '@/api'

const api = new EventsApi()
const queryApi = new QueryApi()

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventRecordsListRequest) => await api.eventRecordsList(organizationId, projectId, eventListRequest),
    funnelQuery: async (organizationId: number, projectId: number, query: FunnelQuery) => await queryApi.funnelQuery(organizationId, projectId, query)
}

export default dataService
