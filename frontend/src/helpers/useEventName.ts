import {useLexiconStore} from '@/stores/lexicon';
import {EventRef} from '@/types/events';
import { EventType, Event } from '@/api'

export const useEventName = (): (ref: EventRef) => string => {
    const lexiconStore = useLexiconStore();
    return (ref: EventRef): string => {
        const event = ref?.id ? lexiconStore.findEventById(ref.id) : ref?.name ? lexiconStore.findEventByName(ref.name) : {} as Event;
        switch (ref.type) {
            case EventType.Regular:
                return ref?.name || event?.name || '';
            case EventType.Custom:
                return lexiconStore.findCustomEventById(ref.id).name;
        }
        throw new Error('unhandled');
    };
}
