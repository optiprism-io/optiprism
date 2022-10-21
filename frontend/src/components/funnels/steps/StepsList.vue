<template>
    <div class="pf-l-flex pf-m-column">
        <StepItem
            v-for="(step, index) in stepsStore.steps"
            :key="index"
            :index="index"
            :step="step"
        />

        <EventSelector @select="addStep">
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
            >
                {{ $t('common.add_step') }}
            </UiButton>
        </EventSelector>
    </div>
</template>

<script lang="ts" setup>
import {useEventsStore} from '@/stores/eventSegmentation/events';
import StepItem from '@/components/funnels/steps/StepItem.vue';
import {useStepsStore} from '@/stores/funnels/steps';
import EventSelector from '@/components/events/Events/EventSelector.vue';
import {EventRef} from '@/types/events';

const eventsStore = useEventsStore();
const stepsStore = useStepsStore();

const addStep = (ref: EventRef): void => {
    eventsStore.addEventByRef(ref, false);
    stepsStore.addStep({
        events: [{
            event: ref,
            filters: [],
        }]
    })
}
</script>
