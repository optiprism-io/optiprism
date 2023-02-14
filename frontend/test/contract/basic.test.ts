import {describe, expect, test} from 'vitest'
import {config, testRequestWithVariants, InputMaker} from './helpers'
import {stubs} from './stubs'
import {
    AnalysisCumulative,
    AnalysisCumulativeTypeEnum,
    AnalysisLinear,
    AnalysisLinearTypeEnum,
    AnalysisLogarithmic,
    AnalysisLogarithmicTypeEnum,
    AnalysisRollingAverage,
    AnalysisRollingAverageTypeEnum,
    AuthApi,
    BreakdownByProperty,
    BreakdownByPropertyTypeEnum,
    CreateReportRequest,
    CustomEventsApi,
    CustomEventStatus,
    DashboardPanelTypeEnum,
    DashboardsApi,
    DashboardsList200Response,
    DidEventAggregateProperty,
    DidEventAggregatePropertyTypeEnum,
    DidEventCount,
    DidEventCountTypeEnum,
    DidEventHistoricalCount,
    DidEventHistoricalCountTypeEnum,
    DidEventRelativeCount,
    DidEventRelativeCountTypeEnum,
    EventChartType,
    EventFilterByCohort,
    EventFilterByCohortTypeEnum,
    EventFilterByGroup,
    EventFilterByGroupTypeEnum,
    EventFilterByProperty,
    EventFilterByPropertyTypeEnum,
    EventGroupedFilters,
    EventGroupedFiltersGroupsConditionEnum,
    EventGroupedFiltersGroupsInnerFiltersConditionEnum,
    EventPropertiesApi,
    EventRecordRequestEvent,
    EventRecordsApi,
    EventRecordsListRequest,
    EventsApi,
    EventSegmentation,
    EventSegmentationEvent,
    EventsList200Response,
    EventStatus,
    EventType,
    ListResponseMetadataMeta,
    LoginRequest,
    PropertyFilterOperation,
    PropertyType,
    PropertyStatus,
    QueryAggregate,
    QueryAggregatePerGroup,
    QueryAggregateProperty,
    QueryAggregatePropertyPerGroup,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryAggregatePropertyTypeEnum,
    QueryApi,
    QueryCountPerGroup,
    QueryCountPerGroupTypeEnum,
    QueryFormula,
    QueryFormulaTypeEnum,
    QuerySimple,
    QuerySimpleTypeEnum,
    RefreshTokenRequest,
    ReportsApi,
    ReportsList200Response,
    ReportType,
    SegmentConditionDidEvent,
    SegmentConditionDidEventTypeEnum,
    SegmentConditionHadPropertyValue,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHasPropertyValue,
    SegmentConditionHasPropertyValueTypeEnum,
    SignupRequest,
    TimeAfterFirstUse,
    TimeAfterFirstUseTypeEnum,
    TimeBetween,
    TimeBetweenTypeEnum,
    TimeFrom,
    TimeFromTypeEnum,
    TimeLast,
    TimeLastTypeEnum,
    TimeUnit,
    TimeWindowEach,
    TimeWindowEachTypeEnum,
    UpdateCustomEventRequest,
    UpdateEventRequest,
    UpdatePropertyRequest,
    UpdateReportRequest,
    UserPropertiesApi,
    GroupRecordsApi,
    GroupRecordsListRequest,
    EventRecordsList200Response,
    GroupRecordsList200Response,
    UpdateGroupRecordRequest,
    PropertyValuesApi,
    ListPropertyValuesRequest,
    PropertyValuesRequestFilter, PropertyValuesList200Response
} from '../../src/api';

describe('Unauthorized', () => {
    describe('Auth', () => {
        const authApi = new AuthApi(config());

        test('Login', async () => {
            await expect(authApi.basicLogin(<LoginRequest>{
                email: 'email',
                password: 'password'
            })).toBeApiResponse(stubs.tokenResponse)
        })
        test('Signup', async () => {
            await expect(authApi.basicSignup(<SignupRequest>{
                email: 'email',
                password: 'password',
                passwordRepeat: 'password',
                firstName: 'first name',
                lastName: 'last name'
            })).toBeApiResponse(stubs.tokenResponse)
        })

        test('Refresh Token', async () => {
            await expect(authApi.refreshToken(<RefreshTokenRequest>{refreshToken: 'refresh_token'})).toBeApiResponse(stubs.tokenResponse)
        })
    })
})

describe('Authorized', () => {
    describe('Dashboards', () => {
        const api = new DashboardsApi(config({auth: true}))

        test('List dashboards', async () => {
            await expect(api.dashboardsList(1, 1)).toBeApiResponse(<DashboardsList200Response>{
                data: [stubs.dashboard],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })


        test('Create dashboard', async () => {
            await expect(api.createDashboard(1, 1, {
                tags: ['d'],
                name: 'test',
                description: 'desc',
                panels: [
                    {
                        type: DashboardPanelTypeEnum.Report,
                        reportId: 1,
                        x: 1,
                        y: 2,
                        w: 3,
                        h: 4,
                    }
                ]
            })).toBeApiResponse(stubs.dashboard);
        })

        test('Get dashboard by id', async () => {
            await expect(api.getDashboard(1, 1, 1)).toBeApiResponse(stubs.dashboard);
        })

        test('Update dashboard', async () => {
            await expect(api.updateDashboard(1, 1, 1, {
                tags: ['d'],
                name: 'test',
                description: 'desc',
                panels: [
                    {
                        type: DashboardPanelTypeEnum.Report,
                        reportId: 1,
                        x: 1,
                        y: 2,
                        w: 3,
                        h: 4,
                    }
                ]
            })).toBeApiResponse(stubs.dashboard);
        })

        test('Delete dashboard', async () => {
            await expect(api.deleteDashboard(1, 1, 1)).toBeApiResponse(stubs.dashboard);
        })
    })

    describe('Reports', () => {
        const api = new ReportsApi(config({auth: true}))

        test('List reports', async () => {
            await expect(api.reportsList(1, 1)).toBeApiResponse(<ReportsList200Response>{
                data: [stubs.report],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Create report', async () => {
            await expect(api.createReport(1, 1, <CreateReportRequest>{
                tags: ['tag'],
                name: 'name',
                description: 'description',
                type: ReportType.EventSegmentation,
                query: <EventSegmentation>{
                    time: <TimeFrom>{
                        type: TimeFromTypeEnum.From,
                        from: new Date(1).toISOString(),
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
            })).toBeApiResponse(stubs.report);
        })

        test('Get report by id', async () => {
            await expect(api.getReport(1, 1, 1)).toBeApiResponse(stubs.report);
        })

        test('Update report', async () => {
            await expect(api.updateReport(1, 1, 1, <UpdateReportRequest>{
                tags: ['tag'],
                name: 'name',
                description: 'description',
                type: ReportType.EventSegmentation,
                query: <EventSegmentation>{
                    time: <TimeFrom>{
                        type: TimeFromTypeEnum.From,
                        from: new Date(1).toISOString(),
                    },
                    group: 'group',
                    intervalUnit: TimeUnit.Day,
                    chartType: EventChartType.Bar,
                    analysis: <AnalysisCumulative>{type: AnalysisCumulativeTypeEnum.Cumulative},
                    events: [<EventSegmentationEvent>{
                        eventType: EventType.Regular,
                        eventName: 'event',
                        queries: [<QuerySimple>{type: QuerySimpleTypeEnum.CountEvents}]
                    }]

                }
            })).toBeApiResponse(stubs.report);
        })

        test('Delete report', async () => {
            await expect(api.deleteReport(1, 1, 1)).toBeApiResponse(stubs.report);
        })
    })

    describe('Events', () => {
        const api = new EventsApi(config({auth: true}))

        test('List events', async () => {
            await expect(api.eventsList(1, 1)).toBeApiResponse(<EventsList200Response>{
                data: [stubs.event],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get event by id', async () => {
            await expect(api.getEvent(1, 1, 1)).toBeApiResponse(stubs.event);
        })

        test('Update event', async () => {
            await expect(api.updateEvent(1, 1, 1, <UpdateEventRequest>{
                tags: ['tag'],
                displayName: 'display_name',
                description: 'description',
                status: EventStatus.Disabled,
            })).toBeApiResponse(stubs.event);
        })
    })

    describe('Custom Events', () => {
        const api = new CustomEventsApi(config({auth: true}))

        test('List custom events', async () => {
            await expect(api.customEventsList(1, 1)).toBeApiResponse(<EventsList200Response>{
                data: [stubs.customEvent],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get custom event by id', async () => {
            await expect(api.getCustomEvent(1, 1, 1)).toBeApiResponse(stubs.customEvent);
        })

        test('Update event', async () => {
            await expect(api.updateCustomEvent(1, 1, 1, <UpdateCustomEventRequest>{
                tags: ['tag'],
                name: 'name',
                description: 'description',
                status: CustomEventStatus.Disabled,
                events: stubs.customEvent.events,
            })).toBeApiResponse(stubs.customEvent);
        })

        test('Delete custom event', async () => {
            await expect(api.deleteCustomEvent(1, 1, 1)).toBeApiResponse(stubs.customEvent);
        })
    })

    describe('User Properties', () => {
        const api = new UserPropertiesApi(config({auth: true}))

        test('List user properties', async () => {
            await expect(api.userPropertiesList(1, 1)).toBeApiResponse(<EventsList200Response>{
                data: [stubs.userProperty],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get user property by id', async () => {
            await expect(api.getUserProperty(1, 1, 1)).toBeApiResponse(stubs.userProperty);
        })

        test('Update user property', async () => {
            await expect(api.updateUserProperty(1, 1, 1, <UpdatePropertyRequest>{
                tags: ['tag'],
                displayName: 'name',
                description: 'description',
                status: PropertyStatus.Disabled,
            })).toBeApiResponse(stubs.userProperty);
        })
    })

    describe('Event Properties', () => {
        const api = new EventPropertiesApi(config({auth: true}))

        test('List event properties', async () => {
            await expect(api.eventPropertiesList(1, 1)).toBeApiResponse(<EventsList200Response>{
                data: [stubs.userProperty],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get event property by id', async () => {
            await expect(api.getEventProperty(1, 1, 1)).toBeApiResponse(stubs.userProperty);
        })

        test('Update event property', async () => {
            await expect(api.updateEventProperty(1, 1, 1, <UpdatePropertyRequest>{
                tags: ['tag'],
                displayName: 'name',
                description: 'description',
                status: PropertyStatus.Disabled,
            })).toBeApiResponse(stubs.userProperty);
        })
    })

    describe('Event Records', () => {
        const api = new EventRecordsApi(config({auth: true}))

        test('List event records', async () => {
            await expect(api.eventRecordsList(1, 1, <EventRecordsListRequest>{
                time: <TimeLast>{last: 1, unit: TimeUnit.Day, type: TimeLastTypeEnum.Last},
                searchInEventProperties: ['test'],
                searchInUserProperties: ['test'],
                events: [<EventRecordRequestEvent>{
                    eventName: 'event',
                    eventType: EventType.Regular,
                    filters: [stubs.eventFilterByProperty]
                }],
                filters: stubs.eventGroupedFilters,
            })).toBeApiResponse(<EventRecordsList200Response>{
                data: [stubs.eventRecord],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get event record by id', async () => {
            await expect(api.getEventRecord(1, 1, 1)).toBeApiResponse(stubs.eventRecord);
        })
    })

    describe('Group Records', () => {
        const api = new GroupRecordsApi(config({auth: true}))

        test('List group records', async () => {
            await expect(api.groupRecordsList(1, 1, <GroupRecordsListRequest>{
                time: <TimeLast>{last: 1, unit: TimeUnit.Day, type: TimeLastTypeEnum.Last},
                group: 'group',
                searchTerm: 'term',
                segments: [stubs.segment],
                filters: stubs.eventGroupedFilters,
            })).toBeApiResponse(<GroupRecordsList200Response>{
                data: [stubs.groupRecord],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })

        test('Get group record by id', async () => {
            await expect(api.getGroupRecord(1, 1, 1)).toBeApiResponse(stubs.groupRecord);
        })

        test('Update group record', async () => {
            await expect(api.updateGroupRecord(1, 1, 1, <UpdateGroupRecordRequest>{
                properties: {'prop': 1}
            })).toBeApiResponse(stubs.groupRecord);
        })
    })

    describe('Property Values', () => {
        const api = new PropertyValuesApi(config({auth: true}))
        test('List property values', async () => {
            await expect(api.propertyValuesList(1, 1, <ListPropertyValuesRequest>{
                eventType: EventType.Regular,
                eventName: 'event',
                propertyType: PropertyType.User,
                propertyName: 'property',
                filter: <PropertyValuesRequestFilter>{
                    operation: PropertyFilterOperation.Eq,
                    value: [1, '2']
                }
            })).toBeApiResponse(<PropertyValuesList200Response>{
                data: [stubs.propertyValue],
                meta: <ListResponseMetadataMeta>{next: 'next'}
            })
        })
    })

    describe('queries', () => {
        describe('Event Segmentation', () => {
            const queryApi = new QueryApi(config({auth: true}))

            testRequestWithVariants(
                (req: any) => {
                    return queryApi.eventSegmentationQuery(1, 1, req)
                },
                stubs.dataTable,
                (im: InputMaker) => {
                    {
                        return <EventSegmentation>{
                            time: im.make(1, [
                                <TimeBetween>{
                                    type: TimeBetweenTypeEnum.Between,
                                    from: new Date(1).toISOString(),
                                    to: new Date(1).toISOString(),
                                },
                                <TimeFrom>{
                                    type: TimeFromTypeEnum.From,
                                    from: new Date(1).toISOString(),
                                },
                                <TimeLast>{
                                    type: TimeLastTypeEnum.Last,
                                    last: 1,
                                    unit: TimeUnit.Day,
                                }
                            ]),
                            chartType: EventChartType.Bar,
                            analysis: im.make(2, [
                                <AnalysisLinear>{
                                    type: AnalysisLinearTypeEnum.Linear
                                },
                                <AnalysisRollingAverage>{
                                    type: AnalysisRollingAverageTypeEnum.RollingAverage,
                                    window: 10,
                                    unit: TimeUnit.Hour,
                                },
                                <AnalysisLogarithmic>{
                                    type: AnalysisLogarithmicTypeEnum.Logarithmic,
                                },
                                <AnalysisCumulative>{
                                    type: AnalysisCumulativeTypeEnum.Cumulative
                                }
                            ]),
                            compare: im.make(3, [null, {
                                offset: 1,
                                unit: TimeUnit.Day
                            }]),
                            group: 'users',
                            intervalUnit: TimeUnit.Day,
                            events: [
                                {
                                    eventType: EventType.Regular,
                                    eventName: 'event',
                                    queries: [
                                        <QuerySimple>{
                                            type: QuerySimpleTypeEnum.CountEvents,
                                        },
                                    ]
                                },
                                {
                                    eventType: EventType.Regular,
                                    eventName: 'event',
                                    filters: [
                                        <EventFilterByProperty>{
                                            propertyType: PropertyType.Custom,
                                            propertyId: 1,
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1, '2', true]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Empty
                                        }
                                    ],
                                    breakdowns: [
                                        <BreakdownByProperty>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop',
                                            type: BreakdownByPropertyTypeEnum.Property
                                        }
                                    ],
                                    queries: [
                                        <QuerySimple>{
                                            type: QuerySimpleTypeEnum.CountEvents,
                                        },
                                        <QueryCountPerGroup>{
                                            type: QueryCountPerGroupTypeEnum.CountPerGroup,
                                            aggregate: QueryAggregate.Avg
                                        },
                                        <QueryAggregatePropertyPerGroup>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop',
                                            type: QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup,
                                            aggregate: QueryAggregate.Avg,
                                            aggregatePerGroup: QueryAggregatePerGroup.Max,
                                        },
                                        <QueryAggregateProperty>{
                                            propertyType: PropertyType.Event,
                                            propertyName: 'prop2',
                                            type: QueryAggregatePropertyTypeEnum.AggregateProperty,
                                            aggregate: QueryAggregate.Avg
                                        },
                                        <QueryFormula>{
                                            type: QueryFormulaTypeEnum.Formula,
                                            formula: 'formula'
                                        }
                                    ]
                                }],
                            breakdowns: [
                                <BreakdownByProperty>{
                                    propertyType: PropertyType.Event,
                                    propertyName: 'prop',
                                    type: BreakdownByPropertyTypeEnum.Property
                                }
                            ],
                            filters: <EventGroupedFilters>{
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
                            },
                            segments: [
                                {
                                    name: 's1',
                                    conditions: [
                                        <SegmentConditionHasPropertyValue>{
                                            type: SegmentConditionHasPropertyValueTypeEnum.HasPropertyValue,
                                            propertyType: PropertyType.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Eq,
                                            values: [1]
                                        },
                                        <SegmentConditionHadPropertyValue>{
                                            type: SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue,
                                            propertyType: PropertyType.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Empty,
                                            time: <TimeBetween>{
                                                type: TimeBetweenTypeEnum.Between,
                                                from: new Date(1).toISOString(),
                                                to: new Date(1).toISOString(),
                                            }
                                        },
                                        <SegmentConditionHadPropertyValue>{
                                            type: SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue,
                                            propertyType: PropertyType.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Eq,
                                            values: [1],
                                            time: <TimeWindowEach>{
                                                type: TimeWindowEachTypeEnum.WindowEach,
                                                unit: TimeUnit.Day,
                                            }
                                        },
                                        <SegmentConditionDidEvent>{
                                            eventType: EventType.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyType.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventCount>{
                                                type: DidEventCountTypeEnum.Count,
                                                operation: PropertyFilterOperation.Eq,
                                                value: 1,
                                                time: <TimeAfterFirstUse>{
                                                    type: TimeAfterFirstUseTypeEnum.AfterFirstUse,
                                                    within: 1,
                                                    unit: TimeUnit.Day,
                                                }
                                            },
                                        },
                                        <SegmentConditionDidEvent>{
                                            eventType: EventType.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyType.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventRelativeCount>{
                                                eventType: EventType.Regular,
                                                eventName: 'right event',
                                                type: DidEventRelativeCountTypeEnum.RelativeCount,
                                                filters: [
                                                    <EventFilterByProperty>{
                                                        propertyType: PropertyType.Custom,
                                                        propertyId: 1,
                                                        type: EventFilterByPropertyTypeEnum.Property,
                                                        operation: PropertyFilterOperation.Eq,
                                                        value: [1]
                                                    },
                                                ],
                                                operation: PropertyFilterOperation.Eq,
                                                time: <TimeAfterFirstUse>{
                                                    type: TimeAfterFirstUseTypeEnum.AfterFirstUse,
                                                    within: 1,
                                                    unit: TimeUnit.Day,
                                                }
                                            },
                                        },
                                        <SegmentConditionDidEvent>{
                                            eventType: EventType.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyType.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventAggregateProperty>{
                                                type: DidEventAggregatePropertyTypeEnum.AggregateProperty,
                                                propertyType: PropertyType.User,
                                                propertyName: 'prop',
                                                aggregate: QueryAggregate.Max,
                                                operation: PropertyFilterOperation.Gt,
                                                value: 0.1,
                                                time: <TimeAfterFirstUse>{
                                                    type: TimeAfterFirstUseTypeEnum.AfterFirstUse,
                                                    within: 1,
                                                    unit: TimeUnit.Day,
                                                }
                                            },
                                        },
                                        <SegmentConditionDidEvent>{
                                            eventType: EventType.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyType.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventHistoricalCount>{
                                                type: DidEventHistoricalCountTypeEnum.HistoricalCount,
                                                operation: PropertyFilterOperation.Gt,
                                                value: 1,
                                                time: <TimeAfterFirstUse>{
                                                    type: TimeAfterFirstUseTypeEnum.AfterFirstUse,
                                                    within: 1,
                                                    unit: TimeUnit.Day,
                                                }
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            )
        })
    })
})