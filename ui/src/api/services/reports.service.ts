import {
    EventSegmentation,
    ReportsApi,
    CreateReportRequest,
    UpdateReportRequest,
} from '@/api'

const api = new ReportsApi()

const schemaReports = {
    eventSegmentation:  async(organizationId: number, projectId: number, eventSegmentation: EventSegmentation) => await api.eventSegmentationQuery(organizationId, projectId, eventSegmentation),

    reportsList: async(organizationId: number, projectId: number) => await api.reportsList(organizationId, projectId),
    getReport: async(organizationId: number, projectId: number, reportId: number) => await api.getReport(organizationId, projectId, reportId),
    createReport: async(organizationId: number, projectId: number, createReportRequest: CreateReportRequest) => await api.createReport(organizationId, projectId, createReportRequest),
    deleteReport: async(organizationId: number, projectId: number, reportId: number) => await api.deleteReport(organizationId, projectId, reportId),
    updateReport: async(organizationId: number, projectId: number, reportId: number, updateReportRequest: UpdateReportRequest) => await api.updateReport(organizationId, projectId, reportId, updateReportRequest),
}

export default schemaReports
