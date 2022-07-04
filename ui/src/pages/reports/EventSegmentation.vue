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
            <EventsViews
                :event-segmentation="eventSegmentation"
                :loading="eventSegmentationLoading"
                @get-event-segmentation="getEventSegmentation"
            />
        </template>
    </ToolsLayout>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import Events from '@/components/events/Events/Events.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Filters from '@/components/events/Filters.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import EventsViews from '@/components/events/EventsViews.vue';
import CreateCustomEvent from '@/components/events/CreateCustomEvent.vue'
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import { DataTableResponse } from '@/api'
import queriesService from '@/api/services/queries.service'

import { useEventsStore } from '@/stores/eventSegmentation/events';
import { useCommonStore } from '@/stores/common'
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';

const eventsStore = useEventsStore();

const eventSegmentationLoading = ref(false)
const eventSegmentation = ref<DataTableResponse>()

onMounted(async () => {
    await lexiconStore.getEvents();
    await lexiconStore.getEventProperties();
    await lexiconStore.getUserProperties();

    await eventsStore.initPeriod();
});

onUnmounted(() => {
    eventsStore.$reset();
});

const getEventSegmentation = async () => {
    eventSegmentationLoading.value = true
    try {
        const res: DataTableResponse = await queriesService.eventSegmentation(eventsStore.propsForEventSegmentationResult)
        if (res) {
            eventSegmentation.value = res
        }
    } catch (error) {
        throw new Error('error Get Event Segmentation')
    }
    eventSegmentationLoading.value = false
}
</script>

<style scoped lang="scss">
.page-title {
    color: var(--op-base-color);
    font-size: 1.4rem;
    margin-bottom: .2rem;
}
</style>
