<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.event_segmentation') }}
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.events') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <Events @get-event-segmentation="getEventSegmentation" />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.segments.label') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <Segments @get-event-segmentation="getEventSegmentation" />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.filters') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <Filters />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.breakdowns') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <Breakdowns />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col">
            <EventsViews
                :event-segmentation="eventSegmentation"
                :loading="eventSegmentationLoading"
                @get-event-segmentation="getEventSegmentation"
            />
        </div>
    </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import Events from '@/components/events/Events/Events.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Filters from '@/components/events/Filters.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import EventsViews from '@/components/events/EventsViews.vue';
import { DataTableResponse } from '@/api'
import queriesService from '@/api/services/queries.service'

import { useLexiconStore } from '@/stores/lexicon';
import { useEventsStore } from '@/stores/eventSegmentation/events';

const lexiconStore = useLexiconStore();
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
