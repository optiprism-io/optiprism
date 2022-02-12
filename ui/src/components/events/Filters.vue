<template>
    <div class="pf-l-flex pf-m-column">
        <Filter
            v-for="(filter, i) in filters"
            :key="i"
            :filter="filter"
            :index="i"
            :show-identifier="true"
            :event-refs="eventRefs"
            @remove-filter="removeFilter"
            @change-filter-property="changeFilterRef"
            @change-filter-operation="changeFilterOperation"
            @add-filter-value="addFilterValue"
            @remove-filter-value="removeFilterValue"
        />
        <div class="pf-l-flex">
            <PropertySelect
                :event-refs="eventRefs"
                @select="addFilter"
            >
                <UiButton
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                >
                    Add Filter
                </UiButton>
            </PropertySelect>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { OperationId, Value } from "@/types";
import Filter from "@/components/events/Filter.vue";
import PropertySelect from "@/components/events/PropertySelect.vue";
import { useFiltersStore } from "@/stores/eventSegmentation/filters";
import { useEventsStore } from "@/stores/eventSegmentation/events";
import { PropertyRef } from "@/types/events";
const filtersStore = useFiltersStore();
const eventsStore = useEventsStore();

const filters = computed(() => filtersStore.filters.map(item => {
    return {
        ...item,
        error: !eventsStore.allSelectedEventPropertyRefs.find(ref => JSON.stringify(ref) === JSON.stringify(item.propRef)),
    };
}));
const eventRefs = computed(() => eventsStore.events.map(item => item.ref));

const addFilter = (ref: PropertyRef): void => {
    filtersStore.addFilter(ref);
};

const removeFilter = (idx: number): void => {
    filtersStore.removeFilter(idx);
};

const changeFilterRef = (filterIdx: number, ref: PropertyRef) => {
    filtersStore.changeFilterRef(filterIdx, ref);
};

const changeFilterOperation = (filterIdx: number, opId: OperationId) => {
    filtersStore.changeFilterOperation(filterIdx, opId);
};

const addFilterValue = (filterIdx: number, value: Value) => {
    filtersStore.addFilterValue(filterIdx, value);
};

const removeFilterValue = (filterIdx: number, value: Value) => {
    filtersStore.removeFilterValue(filterIdx, value);
};
</script>
