<template>
    <Select
        grouped
        :is-open-mount="isOpenMount"
        :items="items"
        :selected="selected"
        :update-open="updateOpen"
        :width-auto="true"
        @select="select"
    >
        <slot />
    </Select>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import {
    EventCustomProperty,
    EventRef,
    PropertyRef,
} from '@/types/events';
import Select from '@/components/Select/Select.vue';
import { Group, Item } from '@/components/Select/SelectTypes';
import { useLexiconStore } from '@/stores/lexicon';
import { PropertyType, EventType, Property, CustomProperty } from '@/api'

const lexiconStore = useLexiconStore();

const emit = defineEmits<{
    (e: 'select', ref: PropertyRef): void;
}>();

const props = defineProps<{
    eventRef?: EventRef;
    eventRefs?: EventRef[];
    selected?: PropertyRef;
    isOpenMount?: boolean;
    updateOpen?: boolean;
    disabledItems?: any[];
    forceProps?: Property[];
}>();

const checkDisable = (propRef: PropertyRef): boolean => {
    return props.disabledItems ? Boolean(props.disabledItems.find((item) => JSON.stringify(item.propRef) === JSON.stringify(propRef))) : false;
};

const getEventProperties = (eventRef: EventRef) => {
    const properties: Group<Item<PropertyRef, null>[]>[] = [];

    if (eventRef.type === EventType.Regular) {
        const eventProperties = lexiconStore.findEventProperties(eventRef)

        if (eventProperties.length) {
            const items: Item<PropertyRef, null>[] = [];
            eventProperties.forEach((prop: Property): void => {
                const propertyRef: PropertyRef = {
                    type: PropertyType.Event,
                    id: prop.id
                };

                items.push({
                    item: propertyRef,
                    name: prop.name,
                    disabled: checkDisable(propertyRef),
                });
            });
            properties.push({ name: 'Event Properties', items, });
        }

        const eventCustomProperties = lexiconStore.findEventCustomProperties(eventRef);

        if (eventCustomProperties.length) {
            const items: Item<PropertyRef, null>[] = [];

            eventCustomProperties.forEach((prop: CustomProperty): void => {
                if (prop.id) {
                    const propertyRef: PropertyRef = {
                        type: PropertyType.Custom,
                        id: prop.id
                    }

                    items.push({
                        item: propertyRef,
                        name: prop.name || '',
                        disabled: checkDisable(propertyRef),
                    })
                }
            });
            properties.push({
                name: 'Event Custom Properties',
                items: items
            });
        }
    }

    return properties;
}

const items = computed(() => {
    let ret: Group<Item<PropertyRef, null>[]>[] = [];

    if (lexiconStore.eventProperties.length) {
        const items: Item<PropertyRef, null>[] = [];
        lexiconStore.eventProperties.forEach((prop: Property): void => {
            const propertyRef: PropertyRef = {
                type: PropertyType.Event,
                id: prop.id
            };

            items.push({
                item: propertyRef,
                name: prop.name,
                disabled: checkDisable(propertyRef),
                description: prop?.description
            });
        });
        ret.push({ name: 'Event Properties', items: items });
    }

    if (lexiconStore.userProperties.length) {
        const items: Item<PropertyRef, null>[] = [];
        lexiconStore.userProperties.forEach((prop: Property): void => {
            const propertyRef: PropertyRef = {
                type: PropertyType.User,
                id: prop.id
            };

            items.push({
                item: propertyRef,
                name: prop.name,
                disabled: checkDisable(propertyRef),
                description: prop?.description
            });
        });
        ret.push({ name: 'User Properties', items: items });
    }

    if (props.eventRef) {
        ret = [...ret, ...getEventProperties(props.eventRef)];
    }

    if (props.eventRefs) {
        const allEventRefs = props.eventRefs.map(eventRef => {
            return getEventProperties(eventRef)
        });
        ret = [...ret, ...allEventRefs.reduce((refs, item) => {
            item.forEach(itemInner => {
                const existItem = refs.find(ref => ref.name === itemInner.name);

                if (existItem) {
                    itemInner.items.forEach(item => {
                        const i = existItem.items.find(existItemInner => JSON.stringify(existItemInner.item) === JSON.stringify(item.item));

                        if (!i) {
                            existItem.items.push(item);
                        }
                    });
                } else {
                    refs.push(itemInner);
                }
            });

            return refs;
        }, [])];
    }

    return ret;
});

const select = (item: PropertyRef) => {
    emit('select', item);
};
</script>
