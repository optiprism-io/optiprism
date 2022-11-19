import {Dashboard, DashboardPanelTypeEnum, DashboardsApi, DashboardsList200Response} from '../../src/api2'
import {DashboardRow, DashboardPanel} from "../../src/api2";

export const stubs = {
    dashboard: <Dashboard>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        tags: ['tag'],
        name: 'name',
        description: 'description',
        rows: [<DashboardRow>{
            panels: [<DashboardPanel>{
                span: 1,
                type: DashboardPanelTypeEnum.Report,
                reportId: 1
            }]
        }]
    }
}