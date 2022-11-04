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
    AuthApi,
    QueryApi,
    LoginRequest,
    Configuration,
    EventChartType,
    EventRefEventTypeEnum,
    EventSegmentation,
    EventSegmentationEvent,
    QuerySimple,
    QuerySimpleQueryEnum,
    QuerySimpleTypeEnum,
    RefreshTokenRequest,
    TimeBetween,
    TimeBetweenTypeEnum,
    TimeFrom,
    TimeFromTypeEnum,
    TimeLast,
    TimeLastTypeEnum,
    TimeUnit
} from 'api'
import {combineInputs} from './helpers'

const AUTH_HEADER_KEY = 'authorization'
const JWT_KEY = 'access_token_key'

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
            const time: (TimeBetween | TimeFrom | TimeLast)[] = [
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

            const analysis: (AnalysisLinear | AnalysisRollingAverage | AnalysisRollingWindow | AnalysisCumulative)[] = [
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

            const events: EventSegmentationEvent[] = [{
                eventType: EventRefEventTypeEnum.Regular,
                eventName: 'event',
                queries: [
                    <QuerySimple>{
                        type: QuerySimpleTypeEnum.Simple,
                        query: QuerySimpleQueryEnum.CountEvents,
                    }
                ]
            }]

            combineInputs([time, analysis, [null, compare], events], async (vars) => {
                const es: EventSegmentation = {
                    time: vars[0],
                    chartType: EventChartType.Bar,
                    analysis: vars[1],
                    compare: vars[2],
                    group: 'users',
                    intervalUnit: TimeUnit.Day,
                    events: [vars[3]]
                }

                const queryApi = new QueryApi(new Configuration({basePath: import.meta.env.VITE_API_BASE_PATH}));
                await expect(queryApi.eventSegmentationQuery(1, 1, es)).resolves.not.toThrow()

            })


        })
    })
})
