import { fetch } from '../apiClient'
import { TimeUnit } from '@/api'

type Breakdown = {
    breakdownType: string
    propertyName: string
    propertyType: string
}

type Filter = {
    filterType: string
    propertyName: string
    propertyType: string
    operation: string
}

type Query = {
    queryType: string
    propertyName?: string
    propertyType?: string
    aggregate_per_group?: string
    aggregate?: string
}

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
    breakdowns?: Breakdown[]
    queries: Query[],
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
    breakdowns?: Breakdown[],
    filters?: Filter[],

    segments?: any // TODO integration
}

const schemaEventSegmentation = {
    eventSegmentation: async (params: EventSegmentation) =>
        await fetch('/queries/event-segmentation', 'POST', params),
}

export default schemaEventSegmentation