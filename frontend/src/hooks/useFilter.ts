import schemaService from '@/api/services/schema.service';
import {EventRef, PropertyRef} from '@/types/events';
import {useLexiconStore} from '@/stores/lexicon';
import {
    PropertyValuesList200ResponseValues,
    PropertyValuesListRequestEventTypeEnum,
} from '@/api'
import { useCommonStore } from '@/stores/common'

interface UseFilter {
    getEventRef: (id: number) => EventRef | undefined;
    getValues: (propRef: PropertyRef) => Promise<PropertyValuesList200ResponseValues>;
}

export const useFilter = (): UseFilter => {
    const lexiconStore = useLexiconStore();
    const commonStore = useCommonStore()

    const getEventRef = (id: number): EventRef | undefined => {
        const event = lexiconStore.events.find(item => {
            if (item.eventProperties) {
                return item.eventProperties.includes(id);
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

    const getValues = async (propRef: PropertyRef): Promise<PropertyValuesList200ResponseValues> => {
        const property = lexiconStore.property(propRef);
        const eventRef = property.id ? getEventRef(property.id) : null
        let valuesList: PropertyValuesList200ResponseValues = []

        try {
            const res = await schemaService.propertyValues(commonStore.organizationId, commonStore.projectId, {
                eventName: eventRef ? lexiconStore.eventName(eventRef) : '',
                eventType: eventRef?.type as PropertyValuesListRequestEventTypeEnum,
                propertyName: property.name || '',
                propertyType: propRef.type
            })

            if (res.data.values) {
                valuesList = res.data.values
            }
        } catch (error) {
            throw new Error('error getEventsValues');
        }

        return valuesList;
    }

    return { getValues, getEventRef };
}
