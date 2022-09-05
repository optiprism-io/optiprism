import {Filter} from '@/types/filters'
import {EventRef} from '@/types/events'

export interface Step {
    events: {
        event: EventRef
        filters: Filter[]
    }[]
}
