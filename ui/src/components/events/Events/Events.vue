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
            :auto-hide="!eventsStore.showCreateCustomEvent"
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
            <Select
                grouped
                :items="lexiconStore.eventsList"
                :width-auto="true"
                :auto-hide="!eventsStore.showCreateCustomEvent"
                @action="selectAction"
                @select="addEvent"
                @edit="editEvent"
                @on-hover="onHoverEvent"
            >
                <UiButton
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                >
                    {{ $t('common.add_event') }}
                </UiButton>
                <template
                    v-if="hoveredCustomEventId"
                    #description
                >
                    <SelectedEvent
                        v-for="(event, index) in hoveredCustomEventDescription"
                        :key="index"
                        :event="event"
                        :event-ref="event.ref"
                        :filters="event.filters"
                        :index="index"
                        :show-breakdowns="false"
                        :show-query="false"
                        :for-preview="true"
                    />
                </template>
            </Select>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed, watch, ref } from "vue";
import { EventQueryRef, EventRef, PropertyRef } from "@/types/events";
import { useEventsStore } from "@/stores/eventSegmentation/events";
import { useLexiconStore } from "@/stores/lexicon";

import Select from '@/components/Select/Select.vue'
import SelectedEvent, { SetEventPayload } from '@/components/events/Events/SelectedEvent.vue'

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();

const events = computed(() => eventsStore.events);

const setEvent = (payload: SetEventPayload) => {
    eventsStore.setEvent(payload)
}

const updateEventSegmentationResult = (): void => {
    eventsStore.fetchEventSegmentationResult()
}

const addEvent = (ref: EventRef) => {
    eventsStore.addEventByRef(ref);
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
        eventsStore.togglePopupCreateCustomEvent(true)
    }
}

const editEvent = (payload: number) => {
    eventsStore.setEditCustomEvent(payload)
    eventsStore.togglePopupCreateCustomEvent(true)
}

const hoveredCustomEventId = ref<number | null>()

const editedEvent = computed(() => {
    if (hoveredCustomEventId.value) {
        return lexiconStore.findCustomEventById(hoveredCustomEventId.value)
    } else {
        return null
    }
})

const hoveredCustomEventDescription = computed(() => {
    if (editedEvent.value && editedEvent.value.events) {
        return editedEvent.value.events.map(item => {
            return {
                ref: item.ref,
                filters: item.filters ? item.filters.map(filter => {
                    return {
                        propRef: filter.propRef,
                        opId: filter.operation,
                        values: filter.value,
                        valuesList: filter.valuesList || [],
                    }
                }) : [],
                breakdowns: [],
                queries: [],
            }
        })
    } else {
        return null
    }
})

const onHoverEvent = (payload: EventRef) => {
    if (payload.type === 'custom') {
        hoveredCustomEventId.value = payload.id
    } else {
        hoveredCustomEventId.value = null
    }
}

watch(eventsStore.events, updateEventSegmentationResult)
</script>
