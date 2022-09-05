import {EventSegmentation, ReportsApi} from '@/api'

const api = new ReportsApi()

const schemaReports = {
    eventSegmentation:  async(organizationId: number, projectId: number, eventSegmentation: EventSegmentation) => await api.eventSegmentationQuery(organizationId, projectId, eventSegmentation),
}

export default schemaReports
