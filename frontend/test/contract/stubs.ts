import {
    AnalysisCumulative,
    AnalysisCumulativeTypeEnum, AnalysisLinear, AnalysisLinearTypeEnum,
    Dashboard,
    DashboardPanel,
    DashboardPanelTypeEnum,
    DashboardRow,
    DataTableResponse,
    DataTableResponseColumnsInnerTypeEnum,
    DataType,
    EventChartType,
    EventSegmentation,
    EventSegmentationEvent,
    EventSegmentationEventEventTypeEnum,
    QuerySimple,
    QuerySimpleTypeEnum,
    Report,
    ReportType,
    TimeFrom,
    TimeFromTypeEnum,
    TimeUnit,
    TokensResponse,
    UpdateReportRequest
} from '../../src/api';

export const stubs = {
    tokenResponse: <TokensResponse>{
        accessToken: 'access_token',
        refreshToken:'refresh_token'
    },
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
    report:<Report>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        tags: ['tag'],
        name: 'name',
        description: 'description',
        type: ReportType.EventSegmentation,
        query: <EventSegmentation>{
            time: <TimeFrom>{
                type: TimeFromTypeEnum.From,
                from: '1970-01-01T00:00:00Z',
            },
            group: 'group',
            intervalUnit: TimeUnit.Second,
            chartType: EventChartType.Line,
            analysis: <AnalysisLinear>{type: AnalysisLinearTypeEnum.Linear},
            events: [<EventSegmentationEvent>{
                eventType: EventSegmentationEventEventTypeEnum.Regular,
                eventName: 'event',
                queries: [<QuerySimple>{type: QuerySimpleTypeEnum.CountEvents}]
            }]

        }
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