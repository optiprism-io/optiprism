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
                        :popper-container="props.popperContainer"
                        :auto-hide="props.autoHide"
                        @select="changeEvent"
                        @action="emit('action', $event)"
                    >
                        <UiButton class="pf-m-main pf-m-secondary">
                            {{ eventName(eventRef) }}
                        </UiButton>
                    </Select>
                </div>
                <div
                    v-if="props.showQuery"
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
                    v-if="props.showBreakdowns"
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
        <div class="selected-event__rows pf-l-flex pf-m-column pf-u-pl-xl">
            <Filter
                v-for="(filter, i) in filters"
                :key="i"
                :event-ref="eventRef"
                :filter="filter"
                :index="i"
                :update-open="updateOpenFilter"
                :popper-container="props.popperContainer"
                @remove-filter="removeFilter"
                @change-filter-property="changeFilterProperty"
                @change-filter-operation="changeFilterOperation"
                @add-filter-value="addFilterValue"
                @remove-filter-value="removeFilterValue"
                @handle-select-property="handleSelectProperty"
            />
            <template v-if="props.showBreakdowns">
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
            </template>
            <template v-if="props.showQuery">
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
            </template>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { EventRef, PropertyRef, PropertyType, EventQueryRef, EVENT_TYPE_REGULAR, EVENT_TYPE_CUSTOM } from "@/types/events";
import { OperationId, Value } from "@/types";
import { useLexiconStore } from "@/stores/lexicon";
import { EventBreakdown, EventFilter, EventQuery, Event, initialQuery } from '@/stores/eventSegmentation/events'
import Select from "@/components/Select/Select.vue";
import Filter from "@/components/events/Filter.vue";
import Breakdown from "@/components/events/Breakdown.vue";
import Query from "@/components/events/Events/Query.vue";
import { Group, Item } from "@/components/Select/SelectTypes";
import AlphabetIdentifier from "@/components/AlphabetIdentifier.vue";
import schemaService from '@/api/services/schema.service'

export type SetEventPayload = {
    event: Event
    index: number
}

type Props = {
    eventRef: EventRef
    event: Event
    filters: EventFilter[]
    breakdowns?: EventBreakdown[]
    eventItems?: Group<Item<EventRef, null>[]>[]
    index: number
    queries?: EventQuery[]
    showBreakdowns?: boolean
    showQuery?: boolean
    popperContainer?: string
    autoHide?: boolean
}

const props = withDefaults(defineProps<Props>(), {
    eventItems: () => [],
    showBreakdowns: true,
    showQuery: true,
    autoHide: true
})

const emit = defineEmits<{
    (e: "changeEvent", index: number, ref: EventRef): void;
    (e: "removeEvent", index: number): void;
    (e: "handleSelectProperty"): void;
    (e: "addBreakdown", index: number): void;
    (e: "changeBreakdownProperty", eventIdx: number, breakdownIdx: number, propRef: PropertyRef): void;
    (e: "removeBreakdown", eventIdx: number, breakdownIdx: number): void;
    (e: "removeQuery", eventIdx: number, queryInx: number): void;
    (e: "addQuery", index: number): void;
    (e: "changeQuery", eventIdx: number, queryIdx: number, queryRef: EventQueryRef): void;

    (e: 'setEvent', payload: SetEventPayload): void
    (e: 'action', payload: string): void
}>();

const lexiconStore = useLexiconStore();

const updateOpenBreakdown = ref(false);
const updateOpenFilter = ref(false);
const updateOpenQuery = ref(false)

const setEvent = (payload: Event) => {
    emit('setEvent', {
        index: props.index,
        event: payload
    })
}

const handleSelectProperty = (): void => {
    emit("handleSelectProperty");
};

const changeEvent = (ref: EventRef): void => {
    setEvent({
        ref: ref,
        filters: [],
        breakdowns: [],
        queries: initialQuery,
    })
}

const removeEvent = (): void => {
    emit('removeEvent', props.index)
};

const removeFilter = (filterIdx: number): void => {
    const event = props.event;

    event.filters.splice(filterIdx, 1)
}

const addFilter = (): void => {
    let event = props.event
    let emptyFilter = event.filters.find((filter): boolean => filter.propRef === undefined)

    if (emptyFilter) {
        return
    }

    event.filters.push({
        opId: OperationId.Eq,
        values: [],
        valuesList: []
    })

    updateOpenFilter.value = true;
    setTimeout(() => {
        updateOpenFilter.value = false;
    });
};

const changeFilterProperty = async (filterIdx: number, propRef: PropertyRef) => {
    let event = props.event
    let valuesList: string[] = []

    try {
        const res = await schemaService.propertiesValues({
            event_name: lexiconStore.eventName(props.event.ref),
            event_type: props.event.ref.type,
            property_name: lexiconStore.propertyName(propRef),
            property_type: PropertyType[propRef.type]
        });
        if (res) {
            valuesList = res
        }
    } catch (error) {
        throw new Error('error getEventsValues')
    }

    event.filters[filterIdx] = {
        propRef: propRef,
        opId: OperationId.Eq,
        values: [],
        valuesList: valuesList
    }
}

const changeFilterOperation = (filterIdx: number, opId: OperationId) => {
    let event = props.event

    event.filters[filterIdx].opId = opId
    event.filters[filterIdx].values = []
}

const addFilterValue = (filterIdx: number, value: Value): void => {
    let event = props.event

    event.filters[filterIdx].values.push(value)
}

const removeFilterValue = (filterIdx: number, value: Value): void => {
    let event = props.event

    event.filters[filterIdx].values = props.event.filters[filterIdx].values.filter(v =>  v !== value)
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
        case EVENT_TYPE_REGULAR:
            return lexiconStore.findEventById(ref.id).displayName;
        case EVENT_TYPE_CUSTOM:
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
