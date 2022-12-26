import {useLexiconStore} from '@/stores/lexicon';
import {EventRef} from '@/types/events';
import { EventType } from '@/api'

export const useEventName = (): (ref: EventRef) => string => {
    const lexiconStore = useLexiconStore();

    return (ref: EventRef): string => {
        const event = lexiconStore.findEventById(ref.id)

        switch (ref.type) {
            case EventType.Regular:
                return event.displayName || event.name
            case EventType.Custom:
                return lexiconStore.findCustomEventById(ref.id).name
        }
        throw new Error('unhandled');
    };
}
