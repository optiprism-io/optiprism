import {describe, expect, test, beforeAll} from 'vitest'

import axios from 'axios'
import jwt from 'jsonwebtoken'
import {isDate} from '../../src/api3/calendarHelper'
import {AuthApi, AuthLoginBody, Configuration, RefreshTokenRequest} from '../../src/api2'
const AUTH_HEADER_KEY = 'authorization'
const JWT_KEY = 'access_token_key'

describe('Unauthorized', () => {
    describe('Auth', () => {
        const authApi = new AuthApi(new Configuration({basePath: import.meta.env.VITE_API_BASE_PATH}));

        test.concurrent('Login', async () => {
            await expect(authApi.basicLogin(<AuthLoginBody>{
                email: 'email',
                password: 'password'
            })).resolves.not.toThrow()
        })
        test.concurrent('Refresh Token', async () => {
            await expect(authApi.refreshToken(<RefreshTokenRequest>{refreshToken: 'refresh_token'})).resolves.not.toThrow()
        })
    })
})
/*

describe('Authorized', () => {
    beforeAll(() => {
        const claims = {
            exp: Math.floor(new Date().getTime() / 1000) + 60 * 60 * 24,
            accountId: 1
        }
        const token = jwt.sign(claims, JWT_KEY, {algorithm: 'HS512'})
        axios.defaults.headers.common[AUTH_HEADER_KEY] = `Bearer ${token}`
    })

    describe('queries', () => {
        test.concurrent('Event Segmentation', async () => {
            const es: EventSegmentation = {
                time: <TimeBetween>{
                    type: TimeBetweenTypeEnum.Between,
                    from: '2017-07-21T17:32:28Z',
                    to: '2017-07-21T17:32:28Z',
                    chartType: EventChartType.Bar,
                    analysis: <AnalysisCumulative>{
                        type: AnalysisCumulativeTypeEnum.Cumulative
                    }
                },
                group: 'users',
                intervalUnit: TimeUnit.Day,
                compare: <EventSegmentationCompare>{
                    offset: 1,
                    unit: TimeUnit.Day
                },
                events: <EventSegmentationEvent[]>[{
                    eventType
                }]
            }
            await expect(queriesService.eventSegmentation(1, 1, es)).resolves.not.toThrow()
        })
    })
})
*/
