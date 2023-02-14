import {
    AnalysisCumulative,
    AnalysisCumulativeTypeEnum,
    AnalysisLinear,
    AnalysisLinearTypeEnum,
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
    QuerySimple,
    QuerySimpleTypeEnum,
    Report,
    ReportType,
    TimeFrom,
    TimeFromTypeEnum,
    TimeUnit,
    TokensResponse,
    UpdateReportRequest,
    Event,
    CustomEvent,
    CustomEventEvent,
    EventType,
    EventFilterByProperty,
    PropertyType,
    EventFilterByPropertyTypeEnum,
    PropertyFilterOperation,
    EventStatus,
    CustomEventStatus,
    Property,
    PropertyStatus,
    DictionaryDataType,
    EventRecord,
    EventGroupedFilters,
    EventGroupedFiltersGroupsConditionEnum,
    EventGroupedFiltersGroupsInnerFiltersConditionEnum,
    EventFilterByCohort,
    EventFilterByCohortTypeEnum,
    EventFilterByGroup,
    EventFilterByGroupTypeEnum,
    SegmentConditionHasPropertyValue,
    SegmentConditionHasPropertyValueTypeEnum, EventSegmentationSegment, GroupRecord
} from '../../src/api';

const eventFilterByProperty = <EventFilterByProperty>{
    type: EventFilterByPropertyTypeEnum.Property,
    propertyType: PropertyType.Event,
    propertyName: 'prop',
    operation: PropertyFilterOperation.Eq,
    value: [1]
};

const eventGroupedFilters = <EventGroupedFilters>{
    groupsCondition: EventGroupedFiltersGroupsConditionEnum.And,
    groups: [
        {
            filtersCondition: EventGroupedFiltersGroupsInnerFiltersConditionEnum.And,
            filters: [
                <EventFilterByCohort>{
                    type: EventFilterByCohortTypeEnum.Cohort,
                    cohortId: 1,
                },
                <EventFilterByProperty>{
                    propertyType: PropertyType.Event,
                    propertyName: 'prop2',
                    type: EventFilterByPropertyTypeEnum.Property,
                    operation: PropertyFilterOperation.Eq,
                    value: [1]
                },
                <EventFilterByGroup>{
                    type: EventFilterByGroupTypeEnum.Group,
                    groupId: 1
                }
            ]
        }
    ]
};

const segment = <EventSegmentationSegment>{
    name: 'test',
    conditions: [<SegmentConditionHasPropertyValue>{
        type: SegmentConditionHasPropertyValueTypeEnum.HasPropertyValue,
        propertyType: PropertyType.User,
        propertyName: 'prop',
        operation: PropertyFilterOperation.Eq,
        values: [1]
    }]
};

export const stubs = {
    eventFilterByProperty: eventFilterByProperty,
    eventGroupedFilters: eventGroupedFilters,
    segment: segment,
    tokenResponse: <TokensResponse>{
        accessToken: 'access_token',
        refreshToken: 'refresh_token'
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
        panels: [
            <DashboardPanel>{
                type: DashboardPanelTypeEnum.Report,
                reportId: 1,
                x: 1,
                y: 2,
                w: 3,
                h: 4,
            }
        ]
    },
    report: <Report>{
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
                eventType: EventType.Regular,
                eventName: 'event',
                queries: [<QuerySimple>{type: QuerySimpleTypeEnum.CountEvents}]
            }]

        }
    },
    event: <Event>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        tags: ['tag'],
        name: 'name',
        displayName: 'display_name',
        description: 'description',
        status: EventStatus.Enabled,
        isSystem: true,
        eventProperties: [1],
        userProperties: [1],
    },
    customEvent: <CustomEvent>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        tags: ['tag'],
        name: 'name',
        description: 'description',
        status: CustomEventStatus.Enabled,
        isSystem: true,
        events: [<CustomEventEvent>{
            eventType: EventType.Custom,
            eventId: 1,
            filters: [eventFilterByProperty]
        }]
    },
    userProperty: <Property>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        events: [1],
        tags: ['tag'],
        name: 'name',
        displayName: 'display_name',
        description: 'description',
        dataType: DataType.Number,
        status: PropertyStatus.Enabled,
        isSystem: true,
        nullable: true,
        isArray: true,
        isDictionary: true,
        dictionaryType: DictionaryDataType.Uint8
    },
    eventProperty: <Property>{
        id: 1,
        createdAt: '1970-01-01T00:00:00Z',
        updatedAt: '1970-01-01T00:00:00Z',
        createdBy: 1,
        updatedBy: 1,
        projectId: 1,
        events: [1],
        tags: ['tag'],
        name: 'name',
        displayName: 'display_name',
        description: 'description',
        dataType: DataType.Number,
        status: PropertyStatus.Enabled,
        isSystem: true,
        nullable: true,
        isArray: true,
        isDictionary: true,
        dictionaryType: DictionaryDataType.Uint8
    },
    eventRecord: <EventRecord>{
        id: 1,
        name: 'name',
        eventProperties: {'key': 'value'},
        userProperties: {'key': 'value'},
        matchedCustomEvents: [1]
    },
    groupRecord: <GroupRecord>{
        id: 1,
        strId: '1',
        group: 'group',
        properties: {'key': 'value'},
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
    },
    propertyValue: 'value'
}