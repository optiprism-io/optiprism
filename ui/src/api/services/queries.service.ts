import { post } from '../apiClient'
import { TimeUnit } from '@/types'

type EventQuery = {
    eventName: string
    eventType: string
    filters?: {
        filterType: string
        propertyName: string
        propertyType: string
        operation?: string
        value?: string[]
    }[]
    breakdowns?: {
        breakdownType: string
        propertyName: string
        propertyType: string
    }[]
    queries: {
        queryType: string
        propertyName?: string
        propertyType?: string
        aggregate_per_group?: string
        aggregate?: string
    }[],
}

export type EventSegmentation = {
    time: {
        type: string
        from: Date
        to: Date
    }
    group: string
    intervalUnit: 'day' | string
    chartType: 'line' | string
    compare?: {
        offset: number
        unit: TimeUnit | string,
    },

    events: EventQuery[],

    // TODO integrations
    breakdowns?: [],
    filters?: [],
}

const schemaEventSegmentation = {
    eventSegmentation: async (params: EventSegmentation) =>
        await post('/queries/event-segmentation', '', params),
}

export default schemaEventSegmentation