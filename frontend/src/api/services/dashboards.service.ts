import {
    DashboardsApi,
    CreateDashboardRequest,
    UpdateDashboardRequest,
} from '@/api'
import {config} from '@/api/services/config';

const api = new DashboardsApi(config)

const schemaDashboards = {
    dashboardsList: async(organizationId: number, projectId: number) => await api.dashboardsList(organizationId, projectId),
    createDashboard: async(organizationId: number, projectId: number, params: CreateDashboardRequest) => await api.createDashboard(organizationId, projectId, params),
    deleteDashboard: async(organizationId: number, projectId: number, dashboardId: number) => await api.deleteDashboard(organizationId, projectId, dashboardId),
    getDashboard: async(organizationId: number, projectId: number, dashboardId: number) => await api.getDashboard(organizationId, projectId, dashboardId),
    updateDashboard: async(organizationId: number, projectId: number, dashboardId: number, updateDashboardRequest: UpdateDashboardRequest) => await api.updateDashboard(organizationId, projectId, dashboardId, updateDashboardRequest),
}

export default schemaDashboards
