import {Configuration, EventRecordsListRequest, EventsApi, EventSegmentation, FunnelQuery, QueryApi} from '@/api'

const api = new EventsApi(new Configuration({ basePath: import.meta.env.VITE_API_BASE_PATH }))
const queryApi = new QueryApi(new Configuration({ basePath: import.meta.env.VITE_API_BASE_PATH }))

const queriesService = {
    createEventsStream: async(organizationId: number, projectId: number, eventListRequest: EventRecordsListRequest) => await api.eventRecordsList(organizationId, projectId, eventListRequest),
    funnelQuery: async (organizationId: number, projectId: number, query: FunnelQuery) => await queryApi.funnelQuery(organizationId, projectId, query),
    eventSegmentation:  async(organizationId: number, projectId: number, eventSegmentation: EventSegmentation) => await queryApi.eventSegmentationQuery(organizationId, projectId, eventSegmentation),

}

export default queriesService
