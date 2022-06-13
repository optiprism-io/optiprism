import schemaService from '@/api/services/schema.service';
import {EventRef, PropertyRef} from '@/types/events';
import {useLexiconStore} from '@/stores/lexicon';

interface UseFilter {
    getEventRef: (id: number) => EventRef | undefined;
    getValues: (propRef: PropertyRef) => Promise<string[]>;
}

export const useFilter = (): UseFilter => {
    const lexiconStore = useLexiconStore();

    const getEventRef = (id: number): EventRef | undefined => {
        const event = lexiconStore.events.find(item => {
            if (item.properties) {
                return item.properties.includes(id);
            }
        });

        let eventRef;

        lexiconStore.eventsList.forEach(item => {
            const eventStoreRef: any = item.items.find(itemInner => itemInner.item.id === event?.id)

            if (event) {
                eventRef = eventStoreRef;
            }
        })

        return eventRef
    };

    const getValues = async (propRef: PropertyRef): Promise<string[]> => {
        const property = lexiconStore.property(propRef);
        const eventRef = getEventRef(property.id)
        let valuesList: string[] = [];

        try {
            const res = await schemaService.propertryValues({
                event_name: eventRef ? lexiconStore.eventName(eventRef) : '',
                event_type: eventRef ? eventRef.type : '',
                property_name: property.name,
                property_type: propRef.type
            });
            if (res) {
                valuesList = res;
            }
        } catch (error) {
            throw new Error('error getEventsValues');
        }

        return valuesList;
    }

    return { getValues, getEventRef };
}
