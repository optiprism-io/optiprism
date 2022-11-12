import {describe, expect, test} from 'vitest'

import jwt from 'jsonwebtoken'
import {Configuration, QueryApi, AuthApi} from 'api'

import {config, InputMaker, jwtToken, testRequestWithVariants} from './helpers'
import {
    AnalysisCumulative,
    AnalysisCumulativeTypeEnum,
    AnalysisLinear,
    AnalysisLinearTypeEnum,
    AnalysisLogarithmic,
    AnalysisLogarithmicTypeEnum,
    AnalysisRollingAverage,
    AnalysisRollingAverageTypeEnum,
    BreakdownByProperty,
    BreakdownByPropertyTypeEnum,
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
    EventGroupedFiltersGroupsFiltersConditionEnum,
    EventRefEventTypeEnum,
    EventSegmentation,
    PropertyFilterOperation,
    PropertyRefPropertyTypeEnum,
    QueryAggregate,
    QueryAggregatePerGroup,
    QueryAggregateProperty,
    QueryAggregatePropertyPerGroup,
    QueryAggregatePropertyPerGroupTypeEnum,
    QueryAggregatePropertyTypeEnum,
    QueryCountPerGroup,
    QueryCountPerGroupTypeEnum,
    QueryFormula,
    QueryFormulaTypeEnum,
    QuerySimple,
    QuerySimpleTypeEnum,
    SegmentConditionDidEvent,
    SegmentConditionDidEventTypeEnum,
    SegmentConditionHadPropertyValue,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHasPropertyValue,
    SegmentConditionHasPropertyValueTypeEnum,
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
    LoginRequest,
    RefreshTokenRequest,
} from '../../packages/api/models'


describe('Unauthorized', () => {
    describe('Auth', () => {
        const authApi = new AuthApi(config());

        test.concurrent('Login', async () => {
            await expect(authApi.basicLogin(<LoginRequest>{
                email: 'email',
                password: 'password'
            })).resolves.not.toThrow()
        })
        test.concurrent('Refresh Token', async () => {
            await expect(authApi.refreshToken(<RefreshTokenRequest>{refreshToken: 'refresh_token'})).resolves.not.toThrow()
        })
    })
})

describe('Authorized', () => {
    describe('queries', () => {
        describe('Event Segmentation', () => {
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
        })
    })
})