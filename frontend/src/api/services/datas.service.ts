import {
    EventRecordsListRequest,
    FunnelQuery,
    QueryApi,
    EventRecordsApi,
} from '@/api'

const queryApi = new QueryApi()
const eventRecordsApi = new EventRecordsApi()

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventRecordsListRequest) => await eventRecordsApi.eventRecordsList(organizationId, projectId, eventListRequest),
    funnelQuery: async (organizationId: number, projectId: number, query: FunnelQuery) => await queryApi.funnelQuery(organizationId, projectId, query)
}

export default dataService
