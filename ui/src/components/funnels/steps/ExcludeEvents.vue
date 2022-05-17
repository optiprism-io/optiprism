<template>
  <div
    v-for="(item, index) in excludedEvents"
    :key="index"
    class="pf-l-flex pf-u-mb-md"
  >
    <span class="pf-l-flex__item">exclude</span>

    <EventSelector
      class="pf-l-flex__item"
      @select="editEvent($event, index)"
    >
      <UiButton
        class="pf-m-main pf-m-secondary"
        is-link
      >
        {{ eventName(item.event) }}
      </UiButton>
    </EventSelector>

    <span class="pf-l-flex__item">between</span>

    <UiSelect
      :items="excludeSteps"
      :show-search="false"
      @update:model-value="editEventSteps($event, index)"
    >
      <UiButton
        class="pf-m-main pf-m-secondary pf-l-flex__item"
        :is-link="true"
      >
        {{ excludeStepsToString(item.steps) }}
      </UiButton>
    </UiSelect>

    <span class="pf-l-flex__item">steps</span>
  </div>

  <EventSelector @select="excludeEvent">
    <UiButton
      class="pf-m-main"
      :is-link="true"
      :before-icon="'fas fa-plus'"
    >
      {{ $t('common.add_step') }}
    </UiButton>
  </EventSelector>
</template>

<script lang="ts" setup>
import {useEventsStore} from '@/stores/eventSegmentation/events';
import {useLexiconStore} from '@/stores/lexicon';
import EventSelector from '@/components/events/Events/EventSelector.vue';
import {computed} from 'vue';
import {ExcludedEventSteps, useStepsStore} from '@/stores/funnels/steps';
import {EventRef} from '@/types/events';
import {EventType} from '@/api';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
const UiSelect = UiSelectGeneric();

const eventsStore = useEventsStore();
const lexiconStore = useLexiconStore();
const stepsStore = useStepsStore();

const excludedEvents = computed(() => stepsStore.excludedEvents)

const excludeSteps = computed<UiSelectItemInterface<string>[]>(() => {
  const groups: UiSelectItemInterface<string>[] = Array
    .from({ length: eventsStore.events.length })
    .map((_, index) => {
      return {
        __type: 'group',
        id: `${index}`,
        label: `${index + 1}`,
        items: Array.from({ length: eventsStore.events.length - index - 1 }).map((_, subIndex) => {
          const idx = index + subIndex + 2;
          return {
            __type: 'item',
            id: `${index}-${subIndex}`,
            label: `${index + 1} and ${idx}`,
            value: `${index + 1}-${idx}`,
          }
        })
      }
    })

  return [
    {
      __type: 'item',
      id: 'all',
      label: 'All',
      value: 'all',
    },
    ...groups
  ]
})

const excludeEvent = (eventRef: EventRef): void => {
  stepsStore.addExcludedEvent({
    event: eventRef,
    steps: { type: 'all' }
  });
}

const editEvent = (eventRef: EventRef, index: number): void => {
  stepsStore.editExcludedEvent({
    index,
    excludedEvent: {
      event: eventRef
    }
  })
}

const editEventSteps = (stepsString: string, index: number): void => {
  const steps = excludeStepsFromString(stepsString);
  stepsStore.editExcludedEvent({
    index,
    excludedEvent: {
      steps
    }
  })
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

const excludeStepsFromString = (stepsString: string): ExcludedEventSteps => {
  if (stepsString === 'all') {
    return {
      type: 'all'
    }
  } else {
    const [from, to] = stepsString.split('-');
    return {
      type: 'between',
      from: Number(from),
      to: Number(to)
    }
  }
}

const excludeStepsToString = (steps: ExcludedEventSteps): string => {
  if (steps.type === 'all') {
    return 'all'
  } else {
    return `${steps.from} and ${steps.to}`
  }
}
</script>
