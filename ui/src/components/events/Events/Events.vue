<template>
    <div class="pf-l-flex pf-m-column">
        <SelectedEvent
            v-for="(event, index) in events"
            :key="index"
            :event-ref="event.ref"
            :filters="event.filters"
            :index="index"
            :event-items="lexiconStore.eventsList"
            :breakdowns="event.breakdowns"
            :queries="event.queries"
            @change-event="changeEvent"
            @remove-event="removeEvent"
            @add-filter="addFilter"
            @remove-filter="removeFilter"
            @change-filter-property="changeFilterProperty"
            @change-filter-operation="changeFilterOperation"
            @add-filter-value="addFilterValue"
            @remove-filter-value="removeFilterValue"
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
                @select="addEvent"
            >
                <UiButton
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                >
                    Add Event
                </UiButton>
            </Select>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { EventQueryRef, EventRef, PropertyRef } from "@/types/events";
import { OperationId, Value } from "@/types";
import { useEventsStore } from "@/stores/eventSegmentation/events";
import { useLexiconStore } from "@/stores/lexicon";
import Select from "@/components/Select/Select.vue";
import SelectedEvent from "@/components/events/Events/SelectedEvent.vue";

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();

const events = computed(() => eventsStore.events);

const addEvent = (ref: EventRef) => {
    eventsStore.addEventByRef(ref);
};

const changeEvent = (index: number, ref: EventRef) => {
    eventsStore.changeEvent(index, ref);
};

const removeEvent = (idx: number): void => {
    eventsStore.deleteEvent(idx);
};

const addFilter = (idx: number): void => {
    eventsStore.addFilter(idx);
};

const removeFilter = (eventIdx: number, filterIdx: number): void => {
    eventsStore.removeFilter(eventIdx, filterIdx);
};

const changeFilterProperty = (eventIdx: number, filterIdx: number, propRef: PropertyRef) => {
    eventsStore.changeFilterProperty(eventIdx, filterIdx, propRef);
};

const changeFilterOperation = (eventIdx: number, filterIdx: number, opId: OperationId) => {
    eventsStore.changeFilterOperation(eventIdx, filterIdx, opId);
};

const addFilterValue = (eventIdx: number, filterIdx: number, value: Value) => {
    eventsStore.addFilterValue(eventIdx, filterIdx, value);
};

const removeFilterValue = (eventIdx: number, filterIdx: number, value: Value) => {
    eventsStore.removeFilterValue(eventIdx, filterIdx, value);
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
</script>
