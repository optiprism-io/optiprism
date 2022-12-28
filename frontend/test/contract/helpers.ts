import {expect, test} from 'vitest';
import jwt from 'jsonwebtoken';
import {Configuration} from '../../src/api';
import {AxiosError} from 'axios';

const JWT_KEY = 'access_token_key'

export const jwtToken = () => {
    const claims = {
        exp: Math.floor(new Date().getTime() / 1000) + 60 * 60 * 24,
        accountId: 1
    }
    return jwt.sign(claims, JWT_KEY, {algorithm: 'HS512'})
}

expect.extend({
    async toBeApiResponse(received: Promise<any>, expected, request=undefined) {
        return await received.then(
            (r) => {
                return {
                    pass: JSON.stringify(r.data) === JSON.stringify(expected),
                    message: () => 'diff error',
                    actual: JSON.stringify(r.data,null,2),
                    expected: JSON.stringify(expected,null,2)
                }
            },
            (e: AxiosError) => {
                const msg = [e.toString()]

                if (request) {
                    msg.push('REQUEST', '=======', JSON.stringify(request), '')
                }
                if (e?.response?.data.error) {
                    msg.push('RESPONSE', '========', JSON.stringify(e.response.data.error, null, 2))
                }

                return {
                    pass: false,
                    message: () => msg.join('\n'),
                }
            },
        );
    }
})

export class InputMaker {
    count = 0
    idx = new Map<number, number>()
    left: number[] = []
    gen: () => any

    constructor(gen: any) {
        this.gen = () => gen(this)
    }

    isDone = () => {
        return this.left.length == 0
    }
    make = (key: number, arr: any[]) => {
        let idx = this.idx.get(key)
        if (idx == undefined) {
            this.idx.set(key, arr.length - 1)
            this.left.push(key)

            return arr[arr.length - 1]
        }

        if (idx > 0) {
            idx--
            this.idx.set(key, idx)

            return arr[idx]
        }

        const index = this.left.indexOf(key, 0);
        if (index > -1) {
            this.left.splice(index, 1);
        }
        return arr[idx]
    }
}

export const testRequestWithVariants = (reqFn: (body: any) => Promise<any>, resp:any, vars: (im: InputMaker) => any) => {
    const im = new InputMaker(vars)
    const reqs = []
    do {
        reqs.push(im.gen())
    } while (!im.isDone())

    test.each(reqs)('request %#', async (req) => {
        await expect(reqFn(req)).toBeApiResponse(resp,req);
    })
}

interface ConfigParameters {
    auth: boolean
}

const defaultTransformer = (reqData, reqHeaders) => {
    console.log('req', reqData)

    return reqData;
}

export const config = (cfg?: ConfigParameters): Configuration => {
    const baseOptions: any = {
        // transformRequest: defaultTransformer,
        // transformResponse: defaultTransformer
    }
    if (cfg?.auth) {
        baseOptions.headers = {Authorization: `Bearer ${jwtToken()}`}
    }
    return new Configuration({
        basePath: import.meta.env.VITE_API_BASE_PATH,
        baseOptions,
    })
}