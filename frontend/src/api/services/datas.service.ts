import {
    EventRecordsListRequest,
    FunnelQuery,
    QueryApi,
    EventRecordsApi,
} from '@/api'
import {config} from '@/api/services/config';

const queryApi = new QueryApi(config)
const eventRecordsApi = new EventRecordsApi(config)

const dataService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventRecordsListRequest) => await eventRecordsApi.eventRecordsList(organizationId, projectId, eventListRequest),
    funnelQuery: async (organizationId: number, projectId: number, query: FunnelQuery) => await queryApi.funnelQuery(organizationId, projectId, query)
}

export default dataService
