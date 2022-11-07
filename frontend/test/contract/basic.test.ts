import {beforeAll, describe, expect, test} from 'vitest'

import axios from 'axios'
import jwt from 'jsonwebtoken'
import {
    AnalysisCumulative,
    AnalysisCumulativeTypeEnum,
    AnalysisLinear,
    AnalysisLinearTypeEnum,
    AnalysisRollingAverage,
    AnalysisRollingAverageTypeEnum,
    AnalysisRollingWindow,
    AnalysisRollingWindowTypeEnum,
    BreakdownByProperty,
    BreakdownByPropertyTypeEnum,
    Configuration,
    EventChartType,
    EventRefEventTypeEnum,
    EventSegmentation,
    EventSegmentationEvent,
    PropertyRefPropertyTypeEnum,
    QueryAggregatePerGroup,
    QueryAggregateProperty,
    QueryAggregatePropertyPerGroup,
    QueryAggregatePropertyTypeEnum,
    QueryApi,
    QueryCountPerGroup,
    QueryCountPerGroupTypeEnum,
    QueryFormula,
    QueryFormulaTypeEnum,
    QuerySimple,
    QuerySimpleQueryEnum,
    QuerySimpleTypeEnum,
    SegmentConditionHasPropertyValue,
    TimeAfterFirstUse,
    TimeBetween,
    TimeBetweenTypeEnum,
    TimeFrom,
    TimeFromTypeEnum,
    TimeLast,
    TimeLastTypeEnum
} from 'api'
import {combineInputs} from './helpers'
import {
    DidEventAggregateProperty,
    DidEventAggregatePropertyPropertyTypeEnum, DidEventAggregatePropertyTypeEnum,
    DidEventCount,
    DidEventCountTypeEnum, DidEventHistoricalCount, DidEventHistoricalCountTypeEnum,
    DidEventRelativeCount,
    DidEventRelativeCountTypeEnum,
    EventFilterByCohort,
    EventFilterByCohortTypeEnum,
    EventFilterByGroup,
    EventFilterByGroupTypeEnum,
    EventFilterByProperty,
    EventFilterByPropertyTypeEnum,
    EventFiltersGroupsConditionEnum,
    EventFiltersGroupsFiltersConditionEnum,
    PropertyFilterOperation,
    QueryAggregate,
    QueryAggregatePropertyPerGroupTypeEnum,
    SegmentConditionDidEvent,
    SegmentConditionDidEventTypeEnum,
    SegmentConditionFunnel,
    SegmentConditionHadPropertyValue,
    SegmentConditionHadPropertyValueTypeEnum,
    SegmentConditionHasPropertyValueTypeEnum,
    TimeAfterFirstUseTypeEnum,
    TimeUnit,
    TimeWindowEach,
    TimeWindowEachTypeEnum
} from '../../packages/api/models'

const AUTH_HEADER_KEY = 'authorization'
const JWT_KEY = 'access_token_key'
/*

describe('Unauthorized', () => {
    describe('Auth', () => {
        const authApi = new AuthApi(new Configuration({basePath: import.meta.env.VITE_API_BASE_PATH}));

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
*/

describe('Authorized', () => {
    beforeAll(() => {
        const claims = {
            exp: Math.floor(new Date().getTime() / 1000) + 60 * 60 * 24,
            accountId: 1
        }
        const token = jwt.sign(claims, JWT_KEY, {algorithm: 'HS512'})
        axios.defaults.headers.common[AUTH_HEADER_KEY] = `Bearer ${token}`
    })

    describe.concurrent('queries', () => {
        test.concurrent('Event Segmentation', async () => {
            const time = [
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
                    n: 1,
                    unit: TimeUnit.Day,
                }
            ];

            const analysis = [
                <AnalysisLinear>{
                    type: AnalysisLinearTypeEnum.Linear
                },
                <AnalysisRollingAverage>{
                    type: AnalysisRollingAverageTypeEnum.RollingAverage,
                    window: 10,
                },
                <AnalysisRollingWindow>{
                    type: AnalysisRollingWindowTypeEnum.RollingWindow,
                    window: 10,
                },
                <AnalysisCumulative>{
                    type: AnalysisCumulativeTypeEnum.Cumulative
                }
            ]

            const compare = {
                offset: 1,
                unit: TimeUnit.Day
            }

            const events: EventSegmentationEvent[] = [
                {
                    eventType: EventRefEventTypeEnum.Regular,
                    eventName: 'event',
                    queries: [
                        <QuerySimple>{
                            type: QuerySimpleTypeEnum.Simple,
                            query: QuerySimpleQueryEnum.CountEvents,
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
                            type: QuerySimpleTypeEnum.Simple,
                            query: QuerySimpleQueryEnum.CountEvents,
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
                }]

            await combineInputs([time, analysis, [null, compare], events], async (vars) => {
                const es: EventSegmentation = {
                    time: vars[0],
                    chartType: EventChartType.Bar,
                    analysis: vars[1],
                    compare: vars[2],
                    group: 'users',
                    intervalUnit: TimeUnit.Day,
                    events: [vars[3]],
                    breakdowns: [
                        <BreakdownByProperty>{
                            propertyType: PropertyRefPropertyTypeEnum.Event,
                            propertyName: 'prop',
                            type: BreakdownByPropertyTypeEnum.Property
                        }
                    ],
                    filters: {
                        groupsCondition: EventFiltersGroupsConditionEnum.And,
                        groups: [
                            {
                                filtersCondition: EventFiltersGroupsFiltersConditionEnum.And,
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
                                    operation: PropertyFilterOperation.Eq,
                                    values: [1],
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
                                        type: DidEventCountTypeEnum.DidEventCount,
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
                                        type: DidEventRelativeCountTypeEnum.DidEventRelativeCount,
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
                                        aggregate:QueryAggregate.Max,
                                        operation:PropertyFilterOperation.Gt,
                                        value:0.1,
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
                                        operation:PropertyFilterOperation.Gt,
                                        value:1,
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

                const queryApi = new QueryApi(new Configuration({basePath: import.meta.env.VITE_API_BASE_PATH}));
                await expect(queryApi.eventSegmentationQuery(1, 1, es)).resolves.not.toThrow()
            })
        })
    })
})
