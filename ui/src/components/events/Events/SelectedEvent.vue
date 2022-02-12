<template>
    <div class="selected-event pf-l-flex pf-m-column">
        <div class="pf-l-flex">
            <AlphabetIdentifier
                class="pf-l-flex__item"
                :index="index"
            />
            <div class="pf-c-action-list">
                <div class="pf-c-action-list__item">
                    <Select
                        grouped
                        :items="eventItems"
                        :selected="eventRef"
                        :width-auto="true"
                        @select="changeEvent"
                    >
                        <UiButton class="pf-m-main pf-m-secondary">
                            {{ eventName(eventRef) }}
                        </UiButton>
                    </Select>
                </div>
                <div
                    class="pf-c-action-list__item selected-event__control"
                    @click="addQuery"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-search" />
                        <template #popper>
                            Add Query
                        </template>
                    </VTooltip>
                </div>
                <div
                    class="pf-c-action-list__item selected-event__control"
                    @click="addFilter"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-filter" />
                        <template #popper>
                            Add Filter
                        </template>
                    </VTooltip>
                </div>
                <div
                    class="pf-c-action-list__item selected-event__control"
                    @click="addBreakdown"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-layer-group" />
                        <template #popper>
                            Add breakdown
                        </template>
                    </VTooltip>
                </div>
                <div
                    class="pf-c-action-list__item selected-event__control"
                    @click="removeEvent"
                >
                    <VTooltip popper-class="ui-hint">
                        <UiIcon icon="fas fa-times" />
                        <template #popper>
                            Remove event
                        </template>
                    </VTooltip>
                </div>
            </div>
        </div>
        <div class="selected-event__rows pf-l-flex pf-m-column pf-u-pl-2xl">
            <Filter
                v-for="(filter, i) in filters"
                :key="i"
                :event-ref="eventRef"
                :filter="filter"
                :index="i"
                :update-open="updateOpenFilter"
                @remove-filter="removeFilter"
                @change-filter-property="changeFilterProperty"
                @change-filter-operation="changeFilterOperation"
                @add-filter-value="addFilterValue"
                @remove-filter-value="removeFilterValue"
                @handle-select-property="handleSelectProperty"
            />
            <Breakdown
                v-for="(breakdown, i) in breakdowns"
                :key="i"
                :event-ref="eventRef"
                :breakdown="breakdown"
                :selected-items="breakdowns"
                :index="i"
                :update-open="updateOpenBreakdown"
                @remove-breakdown="removeBreakdown"
                @change-breakdown-property="changeBreakdownProperty"
            />
            <Query
                v-for="(query, i) in queries"
                :key="i"
                :event-ref="eventRef"
                :item="query"
                :index="i"
                :update-open="updateOpenQuery"
                :no-delete="query.noDelete"
                @remove-query="removeQuery"
                @change-query="changeQuery"
            />
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { EventRef, EventType, PropertyRef, EventQueryRef } from "@/types/events";
import { OperationId, Value } from "@/types";
import { useLexiconStore } from "@/stores/lexicon";
import { EventBreakdown, EventFilter, EventQuery } from "@/stores/eventSegmentation/events";
import Select from "@/components/Select/Select.vue";
import Filter from "@/components/events/Filter.vue";
import Breakdown from "@/components/events/Breakdown.vue";
import Query from "@/components/events/Events/Query.vue";
import { Group, Item } from "@/components/Select/SelectTypes";
import AlphabetIdentifier from "@/components/AlphabetIdentifier.vue";

const props = withDefaults(
    defineProps<{
        eventRef: EventRef;
        filters: EventFilter[];
        breakdowns: EventBreakdown[];
        eventItems: Group<Item<EventRef, null>[]>[];
        index: number;
        queries: EventQuery[];
    }>(), {
        eventItems: () => []
    }
);

const emit = defineEmits<{
    (e: "changeEvent", index: number, ref: EventRef): void;
    (e: "removeEvent", index: number): void;
    (e: "addFilter", index: number): void;
    (e: "removeFilter", eventIdx: number, filterIdx: number): void;
    (e: "changeFilterProperty", eventIdx: number, filterIdx: number, propRef: PropertyRef): void;
    (e: "changeFilterOperation", eventIdx: number, filterIdx: number, opId: OperationId): void;
    (e: "addFilterValue", eventIdx: number, filterIdx: number, value: Value): void;
    (e: "removeFilterValue", eventIdx: number, filterIdx: number, value: Value): void;
    (e: "handleSelectProperty"): void;
    (e: "addBreakdown", index: number): void;
    (e: "changeBreakdownProperty", eventIdx: number, breakdownIdx: number, propRef: PropertyRef): void;
    (e: "removeBreakdown", eventIdx: number, breakdownIdx: number): void;
    (e: "removeQuery", eventIdx: number, queryInx: number): void;
    (e: "addQuery", index: number): void;
    (e: "changeQuery", eventIdx: number, queryIdx: number, queryRef: EventQueryRef): void;
}>();

const lexiconStore = useLexiconStore();

const updateOpenBreakdown = ref(false);
const updateOpenFilter = ref(false);
const updateOpenQuery = ref(false)

const handleSelectProperty = (): void => {
    emit("handleSelectProperty");
};

const changeEvent = (ref: EventRef): void => {
    emit("changeEvent", props.index, ref);
};

const removeEvent = (): void => {
    emit("removeEvent", props.index);
};

const removeFilter = (filterIdx: number): void => {
    emit("removeFilter", props.index, filterIdx);
};

const addFilter = (): void => {
    emit("addFilter", props.index);
    updateOpenFilter.value = true;

    setTimeout(() => {
        updateOpenFilter.value = false;
    });
};

const changeFilterProperty = (filterIdx: number, propRef: PropertyRef): void => {
    emit("changeFilterProperty", props.index, filterIdx, propRef);
};

const changeFilterOperation = (filterIdx: number, opId: OperationId): void => {
    emit("changeFilterOperation", props.index, filterIdx, opId);
};

const addFilterValue = (filterIdx: number, value: Value): void => {
    emit("addFilterValue", props.index, filterIdx, value);
};

const removeFilterValue = (filterIdx: number, value: Value): void => {
    emit("removeFilterValue", props.index, filterIdx, value);
};

const addBreakdown = async (): Promise<void> => {
    await emit("addBreakdown", props.index);

    updateOpenBreakdown.value = true;

    setTimeout(() => {
        updateOpenBreakdown.value = false;
    });
};

const changeBreakdownProperty = (breakdownIdx: number, propRef: PropertyRef): void => {
    emit("changeBreakdownProperty", props.index, breakdownIdx, propRef);
};

const removeBreakdown = (breakdownIdx: number): void => {
    emit("removeBreakdown", props.index, breakdownIdx);
};

const eventName = (ref: EventRef): string => {
    switch (ref.type) {
        case EventType.Regular:
            return lexiconStore.findEventById(ref.id).displayName;
        case EventType.Custom:
            return lexiconStore.findCustomEventById(ref.id).name;
    }
    throw new Error("unhandled");
};

const removeQuery = (idx: number): void => {
    emit("removeQuery", props.index, idx);
};

const addQuery = async (): Promise<void> => {
    await emit("addQuery", props.index);

    updateOpenQuery.value = true;

    setTimeout(() => {
        updateOpenQuery.value = false;
    });
};

const changeQuery = (idx: number, ref: EventQueryRef) => {
    emit("changeQuery", props.index, idx, ref);
};
</script>

<style scoped lang="scss">
.selected-event {
    &__control {
        padding: 5px;
        opacity: 0;
        cursor: pointer;
        color: var(--op-base-color-text);

        &:hover {
            color: var(--pf-global--palette--black-800);
        }
    }

    &:hover {
        .selected-event {
            &__control {
                opacity: 1;
            }
        }
    }
}
</style>
