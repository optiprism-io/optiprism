<template>
    <div class="pf-l-flex pf-m-column">
        <Breakdown
            v-for="(breakdown, i) in breakdowns"
            :key="i"
            :breakdown="breakdown"
            :selected-items="breakdowns"
            :index="i"
            :event-refs="eventRefs"
            @remove-breakdown="removeBreakdown"
            @change-breakdown-property="changeBreakdownProperty"
        />
        <div class="pf-l-flex">
            <PropertySelect
                :event-refs="eventRefs"
                :disabled-items="breakdowns"
                @select="addBreakdown"
            >
                <UiButton
                    class="pf-m-main"
                    :is-link="true"
                    :before-icon="'fas fa-plus'"
                >
                    Add Breakdown
                </UiButton>
            </PropertySelect>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { PropertyRef } from '@/types/events';
import Breakdown from '@/components/events/Breakdown.vue';
import PropertySelect from '@/components/events/PropertySelect.vue';
import { useEventsStore } from '@/stores/eventSegmentation/events';
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
const eventsStore = useEventsStore();
const breakdownsStore = useBreakdownsStore();

const eventRefs = computed(() => eventsStore.events.map(item => item.ref));
const breakdowns = computed(() => breakdownsStore.breakdowns.map(item => {
    return {
        ...item,
        error: !eventsStore.allSelectedEventPropertyRefs.find(ref => JSON.stringify(ref) === JSON.stringify(item.propRef)),
    };
}));

const changeBreakdownProperty = (breakdownIdx: number, propRef: PropertyRef) => {
    breakdownsStore.changeBreakdownProperty(breakdownIdx, propRef);
};

const addBreakdown = (propRef: PropertyRef): void => {
    breakdownsStore.addBreakdown(propRef);
};

const removeBreakdown = (idx: number): void => {
    breakdownsStore.removeBreakdown(idx);
};
</script>
