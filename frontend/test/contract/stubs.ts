import {
    Dashboard,
    DashboardPanel,
    DashboardPanelTypeEnum,
    DashboardRow, DataTableResponse,
    DataTableResponseColumnsInnerTypeEnum, DataType
} from '../../src/api';

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
    },
    dataTable: <DataTableResponse>{
        columns: [
            {
                type: DataTableResponseColumnsInnerTypeEnum.Dimension,
                name: 'name',
                isNullable: true,
                dataType: DataType.Number,
                step: 1,
                data: [1],
                compareValues: [2]
            }

        ]
    }
}