<template>
  <ToolsLayout>
    <template #title>
      {{ $t('events.event_segmentation') }}
    </template>

    <UiCard :title="$t('events.events')">
      <Events />
    </UiCard>

    <UiCard :title="$t('events.segments.label')">
      <Segments />
    </UiCard>

    <UiCard :title="$t('events.filters')">
      <Filters />
    </UiCard>

    <UiCard :title="$t('events.breakdowns')">
      <Breakdowns />
    </UiCard>

    <template #main>
      <EventsViews />
    </template>
  </ToolsLayout>

  <CreateCustomEvent
    v-if="commonStore.showCreateCustomEvent"
    @apply="applyCreateCustomEvent"
    @cancel="togglePopupCreateCustomEvent(false)"
  />
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue';
import Events from '@/components/events/Events/Events.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Filters from '@/components/events/Filters.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import EventsViews from '@/components/events/EventsViews.vue';
import CreateCustomEvent from '@/components/events/CreateCustomEvent.vue'
import UiCard from '@/components/uikit/UiCard/UiCard.vue';

import { useEventsStore } from '@/stores/eventSegmentation/events';
import { useCommonStore } from '@/stores/common'
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';

const eventsStore = useEventsStore();
const commonStore = useCommonStore()

onUnmounted(() => {
  eventsStore.$reset();
});

const togglePopupCreateCustomEvent = (payload: boolean) => {
  commonStore.togglePopupCreateCustomEvent(payload)
}

const applyCreateCustomEvent = () => {
  togglePopupCreateCustomEvent(false)
}
</script>

<style scoped lang="scss">
.page-title {
    color: var(--op-base-color);
    font-size: 1.4rem;
    margin-bottom: .2rem;
}
</style>
