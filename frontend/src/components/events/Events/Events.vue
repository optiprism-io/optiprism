<template>
    <div class="pf-l-flex pf-m-column">
        <SelectedEvent
            v-for="(event, index) in events"
            :key="index"
            :event="event"
            :event-ref="event.ref"
            :filters="event.filters"
            :index="index"
            :event-items="lexiconStore.eventsList"
            :breakdowns="event.breakdowns"
            :queries="event.queries"
            :auto-hide="!commonStore.showCreateCustomEvent"
            :identifier="identifier"
            @action="selectAction"
            @edit="editEvent"
            @set-event="setEvent"
            @remove-event="removeEvent"
            @add-breakdown="addBreakdown"
            @change-breakdown-property="changeBreakdownProperty"
            @remove-breakdown="removeBreakdown"
            @remove-query="removeQuery"
            @add-query="addQuery"
            @change-query="changeQuery"
        />
        <div class="pf-l-flex">
            <EventSelector @select="addEvent">
                <slot name="new" />
            </EventSelector>
        </div>
    </div>
</template>

<script setup lang="ts">
import {computed, watch, defineAsyncComponent, PropType} from 'vue';
import { EventQueryRef, EventRef, PropertyRef } from '@/types/events';
import { useEventsStore, EventPayload } from '@/stores/eventSegmentation/events';
import { useLexiconStore } from '@/stores/lexicon';
import { useCommonStore } from '@/stores/common'
import EventSelector from '@/components/events/Events/EventSelector.vue';
import useCustomEvent from '@/components/events/Events/CustomEventHooks';
const SelectedEvent = defineAsyncComponent(() => import('@/components/events/Events/SelectedEvent.vue'))

const props = defineProps({
    identifier: {
        type: String as PropType<'numeric' | 'alphabet'>,
        default: 'alphabet',
    },
    createWithQuery: {
        type: Boolean,
        default: true,
    },
})

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();
const commonStore = useCommonStore()

const { hoveredCustomEventDescription, hoveredCustomEventId, onHoverEvent } = useCustomEvent()

const emit = defineEmits<{
    (e: 'get-event-segmentation'): void
}>()

const events = computed(() => eventsStore.events);

const setEvent = (payload: EventPayload) => {
    eventsStore.setEvent(payload);
}

const addEvent = (ref: EventRef) => {
    eventsStore.addEventByRef(ref, props.createWithQuery);
};

const removeEvent = (idx: number): void => {
    eventsStore.deleteEvent(idx);
};

const addBreakdown = (idx: number): void => {
    eventsStore.addBreakdown(idx);
};

const changeBreakdownProperty = (eventIdx: number, breakdownIdx: number, propRef: PropertyRef) => {
    eventsStore.changeBreakdownProperty(eventIdx, breakdownIdx, propRef);
};

const removeBreakdown = (eventIdx: number, breakdownIdx: number): void => {
    eventsStore.removeBreakdown(eventIdx, breakdownIdx);
};

const addQuery = (idx: number): void => {
    eventsStore.addQuery(idx);
};

const removeQuery = (eventIdx: number, queryIdx: number): void => {
    eventsStore.removeQuery(eventIdx, queryIdx);
};

const changeQuery = (eventIdx: number, queryIdx: number, ref: EventQueryRef) => {
    eventsStore.changeQuery(eventIdx, queryIdx, ref);
};

const selectAction = (payload: string) => {
    if (payload === 'createCustomEvent') {
        eventsStore.setEditCustomEvent(null)
        commonStore.togglePopupCreateCustomEvent(true)
    }
}

const editEvent = (payload: number) => {
    eventsStore.setEditCustomEvent(Number(payload))
    commonStore.togglePopupCreateCustomEvent(true)
}

watch(eventsStore.events, () => emit('get-event-segmentation'))
</script>
