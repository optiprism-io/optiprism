import {Filter} from '@/stores/eventSegmentation/filters';
import {EventRef} from '@/types/events';

export interface Step {
    eventRef: EventRef;
    filters: Filter[];
}
