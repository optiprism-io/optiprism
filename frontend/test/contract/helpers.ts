import {expect, test} from 'vitest';
import {Configuration} from '../../src/api2';
import jwt from 'jsonwebtoken';

const AUTH_HEADER_KEY = 'authorization'
const JWT_KEY = 'access_token_key'

export const jwtToken = () => {
    const claims = {
        exp: Math.floor(new Date().getTime() / 1000) + 60 * 60 * 24,
        accountId: 1
    }
    return jwt.sign(claims, JWT_KEY, {algorithm: 'HS512'})
}

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

export const testRequest = (resp: Promise<any>) => {
    return resp
        .then((r) => {
            return new Promise((resolve, reject) => resolve(r))
        })
        .catch((e) => {
            console.log(e, JSON.stringify(e.response.data.error, null, 2))
            return new Promise((resolve, reject) => reject(JSON.stringify(e.response.data.error, null, 2)))
        })
}

export const testRequestWithVariants = (reqFn: (body: any) => Promise<any>, vars: (im: InputMaker) => any) => {
    const im = new InputMaker(vars)
    const reqs = []
    do {
        reqs.push(im.gen())
    } while (!im.isDone())

    test.each(reqs)('request %#', async (req) => {
        expect.assertions(1)
        try {
            await reqFn(req)
            expect(true)
        } catch (e) {
            if (e.response?.data?.error) {
                const err = {
                    request: req,
                    error: e.response.data.error
                }
                throw new Error(JSON.stringify(err))
            } else {
                throw new Error(e)
            }
        }
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