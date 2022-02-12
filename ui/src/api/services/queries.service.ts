import { post } from '../apiClient';
import { TimeUnit } from "@/types";

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

    // TODO after integrations
    events?: [],
    breakdowns?: [],
    filters?: [],
}

const schemaEventSegmentation = {
    eventSegmentation: async (params: EventSegmentation) =>
        await post("/queries/event-segmentation", "", params),
}


export default schemaEventSegmentation;