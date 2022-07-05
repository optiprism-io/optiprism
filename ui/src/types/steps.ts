import {Filter} from '@/stores/eventSegmentation/filters';
import {EventRef} from '@/types/events';

export interface Step {
    events: {
        event: EventRef;
        filters: Filter[];
    }[];
}
