<template>
    <div
        v-for="step in stepsStore.steps"
        :key="step.eventRef.id"
    >
        {{ eventName(step.eventRef) }}
    </div>
    <EventSelector @select="addStepWithEvent">
        <UiButton
            class="pf-m-main"
            :is-link="true"
            :before-icon="'fas fa-plus'"
        >
            {{ $t('common.add_step') }}
        </UiButton>
    </EventSelector>
</template>

<script setup lang="ts">
import {useStepsStore} from '@/stores/funnels/steps';
import {useLexiconStore} from '@/stores/lexicon';
import {Step} from '@/types/steps';
import EventSelector from '@/components/events/Events/EventSelector.vue';
import {EventRef} from '@/types/events';
import {EventType} from '@/api';

const stepsStore = useStepsStore();
const lexiconStore = useLexiconStore();

const addStepWithEvent = (eventRef: EventRef) => {
    const step: Step = {
        eventRef: eventRef,
        filters: []
    }
}

const eventName = (ref: EventRef): string => {
    let event = lexiconStore.findEventById(ref.id)

    switch (ref.type) {
        case EventType.Regular:
            return event.displayName || event.name
        case EventType.Custom:
            return lexiconStore.findCustomEventById(ref.id).name
    }
    throw new Error('unhandled');
};
</script>
