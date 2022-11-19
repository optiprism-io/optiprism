import {describe, expect, test} from 'vitest'
// import {DashboardsApi} from '../../packages/api'
import {AxiosResponse} from 'axios'
import {Dashboard, DashboardsApi, DashboardsList200Response, ListResponseMetadataMeta} from '../../src/api2'
import {config, testRequest} from './helpers'
import {DashboardPanelTypeEnum,} from '../../packages/api/models'
import {stubs} from './stubs'
/*
describe('Unauthorized', () => {
    describe('Auth', () => {
        const authApi = new AuthApi(config());

        test.concurrent('Login', () => {
            expect(() => testRequest(authApi.basicLogin(<LoginRequest>{
                email: 'email',
                password: 'password'
            }))).not.toThrow()
        })
        test.concurrent('Signup', () => {
            expect(() => testRequest(authApi.basicSignup(<SignupRequest>{
                email: 'email',
                password: 'password',
                passwordRepeat: 'password',
                firstName: 'first name',
                lastName: 'last name'
            }))).not.toThrow()
        })

        test.concurrent('Refresh Token', () => {
            expect(() => testRequest(authApi.refreshToken(<RefreshTokenRequest>{refreshToken: 'refresh_token'}))).not.toThrow()
        })
    })
})*/

expect.extend({
    async toBeApiResponse(received: Promise<any>, expected) {
        return await received.then(
            (r) => {
                return {
                    pass: JSON.stringify(r.data) === JSON.stringify(expected),
                    message: () => 'error',
                    actual: r.data,
                    expected: expected
                }
            },
            (err) => {
                return {
                    pass: false,
                    message: () => 'error',
                    actual: received,
                    expected: expected
                }
            },
        );
    }
})

describe('Authorized', () => {
    describe('queries', () => {
        describe('Dashboards', () => {
            const api = new DashboardsApi(config({auth: true}))
            test('List', async () => {
                await expect(api.dashboardsList(1, 1)).toBeApiResponse(<DashboardsList200Response>{
                    data: [stubs.dashboard],
                    meta: <ListResponseMetadataMeta>{next: 'next'}
                })
            })


            /* test('Create', async () => {
                 await expect(testRequest(api.createDashboard({
                     tags: ['d'],
                     name: 'test',
                     description: 'desc',
                     rows: [
                         {
                             panels: [
                                 {
                                     span: 1,
                                     type: DashboardPanelTypeEnum.Report,
                                     reportId: 1
                                 }
                             ]
                         }
                     ]
                 }, 1, 1))).resolves.toBe('ok');
             })*/
            /*
                        test.concurrent('Get By Id', () => {
                            expect(() => testRequest(api.getDashboard(1, 1,1))).not.toThrow()
                        })

                        test.concurrent('Update', () => {
                            expect(() => testRequest(api.updateDashboard(<UpdateDashboardRequest>{
                                tags: ['d'],
                                name: 'test',
                                description: 'desc',
                                rows: [
                                    <DashboardRow>{
                                        panels: [
                                            <DashboardPanel>{
                                                span: 1,
                                                type: DashboardPanelTypeEnum.Report,
                                                reportId: 1
                                            }
                                        ]
                                    }
                                ]
                            }, 1, 1, 1))).not.toThrow()
                        })

                        test.concurrent('Get By Id', () => {
                            expect(() => testRequest(api.deleteDashboard(1, 1,1))).not.toThrow()
                        })*/
        })
        /*describe('Event Segmentation', () => {
            const queryApi = new QueryApi(config({auth: true}))

            testRequestWithVariants(
                async (req: any) => {
                    await queryApi.eventSegmentationQuery(1, 1, req)
                },
                (im: InputMaker) => {
                    {
                        return <EventSegmentation>{
                            time: im.make(1, [
                                <TimeBetween>{
                                    type: TimeBetweenTypeEnum.Between,
                                    from: new Date(1),
                                    to: new Date(1),
                                },
                                <TimeFrom>{
                                    type: TimeFromTypeEnum.From,
                                    from: new Date(1),
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
                                    eventType: EventRefEventTypeEnum.Regular,
                                    eventName: 'event',
                                    queries: [
                                        <QuerySimple>{
                                            type: QuerySimpleTypeEnum.CountEvents,
                                        },
                                    ]
                                },
                                {
                                    eventType: EventRefEventTypeEnum.Regular,
                                    eventName: 'event',
                                    filters: [
                                        <EventFilterByProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Custom,
                                            propertyId: 1,
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Eq,
                                            value: [1, '2', true]
                                        },
                                        <EventFilterByProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
                                            propertyName: 'prop',
                                            type: EventFilterByPropertyTypeEnum.Property,
                                            operation: PropertyFilterOperation.Empty
                                        }
                                    ],
                                    breakdowns: [
                                        <BreakdownByProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
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
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
                                            propertyName: 'prop',
                                            type: QueryAggregatePropertyPerGroupTypeEnum.AggregatePropertyPerGroup,
                                            aggregate: QueryAggregate.Avg,
                                            aggregatePerGroup: QueryAggregatePerGroup.Max,
                                        },
                                        <QueryAggregateProperty>{
                                            propertyType: PropertyRefPropertyTypeEnum.Event,
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
                                    propertyType: PropertyRefPropertyTypeEnum.Event,
                                    propertyName: 'prop',
                                    type: BreakdownByPropertyTypeEnum.Property
                                }
                            ],
                            filters: <EventGroupedFilters>{
                                groupsCondition: EventGroupedFiltersGroupsConditionEnum.And,
                                groups: [
                                    {
                                        filtersCondition: EventGroupedFiltersGroupsFiltersConditionEnum.And,
                                        filters: [
                                            <EventFilterByCohort>{
                                                type: EventFilterByCohortTypeEnum.Cohort,
                                                cohortId: 1,
                                            },
                                            <EventFilterByProperty>{
                                                propertyType: PropertyRefPropertyTypeEnum.Event,
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
                                            propertyType: PropertyRefPropertyTypeEnum.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Eq,
                                            values: [1]
                                        },
                                        <SegmentConditionHadPropertyValue>{
                                            type: SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue,
                                            propertyType: PropertyRefPropertyTypeEnum.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Empty,
                                            time: <TimeBetween>{
                                                type: TimeBetweenTypeEnum.Between,
                                                from: new Date(1),
                                                to: new Date(1),
                                            }
                                        },
                                        <SegmentConditionHadPropertyValue>{
                                            type: SegmentConditionHadPropertyValueTypeEnum.HadPropertyValue,
                                            propertyType: PropertyRefPropertyTypeEnum.User,
                                            propertyName: 'prop',
                                            operation: PropertyFilterOperation.Eq,
                                            values: [1],
                                            time: <TimeWindowEach>{
                                                type: TimeWindowEachTypeEnum.WindowEach,
                                                unit: TimeUnit.Day,
                                            }
                                        },
                                        <SegmentConditionDidEvent>{
                                            eventType: EventRefEventTypeEnum.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyRefPropertyTypeEnum.Custom,
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
                                            eventType: EventRefEventTypeEnum.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyRefPropertyTypeEnum.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventRelativeCount>{
                                                eventType: EventRefEventTypeEnum.Regular,
                                                eventName: 'right event',
                                                type: DidEventRelativeCountTypeEnum.RelativeCount,
                                                filters: [
                                                    <EventFilterByProperty>{
                                                        propertyType: PropertyRefPropertyTypeEnum.Custom,
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
                                            eventType: EventRefEventTypeEnum.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyRefPropertyTypeEnum.Custom,
                                                    propertyId: 1,
                                                    type: EventFilterByPropertyTypeEnum.Property,
                                                    operation: PropertyFilterOperation.Eq,
                                                    value: [1]
                                                },
                                            ],
                                            aggregate: <DidEventAggregateProperty>{
                                                type: DidEventAggregatePropertyTypeEnum.AggregateProperty,
                                                propertyType: PropertyRefPropertyTypeEnum.User,
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
                                            eventType: EventRefEventTypeEnum.Regular,
                                            eventName: 'event',
                                            type: SegmentConditionDidEventTypeEnum.DidEvent,
                                            filters: [
                                                <EventFilterByProperty>{
                                                    propertyType: PropertyRefPropertyTypeEnum.Custom,
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
        })*/
    })
})