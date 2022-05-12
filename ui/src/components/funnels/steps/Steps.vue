<template>
  <div
    v-for="(step, index) in stepsStore.steps"
    :key="index"
  >
    {{ step.eventName }}
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

const stepsStore = useStepsStore();
const lexiconStore = useLexiconStore();

const addStepWithEvent = (eventRef: EventRef) => {
  const step: Step = {
    eventRef: eventRef,
    filters: []
  }
}
</script>
