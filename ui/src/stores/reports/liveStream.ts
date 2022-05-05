import { defineStore } from 'pinia'

type LiveStream = {
    controlsPeriod: string
    period: {
        from: string
        to: string
        last: number
        type: string
    }
}

export const useLiveStreamStore = defineStore('liveStream', {
    state: (): LiveStream => ({
        controlsPeriod: '90',
        period: {
            from: '',
            to: '',
            type: 'last',
            last: 30,
        },
    }),
})
