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
                    <Events />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>{{ $t('events.segments.label') }}</p>
                </div>
                <div class="pf-c-card__body">
                    <Segments />
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
            <EventsViews />
        </div>

        <CreateCustomEvent
            v-if="commonStore.showCreateCustomEvent"
            @apply="applyCreateCustomEvent"
            @cancel="togglePopupCreateCustomEvent(false)"
        />
    </div>
</template>

<script setup lang="ts">
import { onBeforeMount, onUnmounted } from "vue";
import Events from "@/components/events/Events/Events.vue";
import Breakdowns from "@/components/events/Breakdowns.vue";
import Filters from "@/components/events/Filters.vue";
import Segments from "@/components/events/Segments/Segments.vue";
import EventsViews from "@/components/events/EventsViews.vue";
import CreateCustomEvent from '@/components/events/CreateCustomEvent.vue'

import { useLexiconStore } from "@/stores/lexicon";
import { useEventsStore } from "@/stores/eventSegmentation/events";
import { useCommonStore } from '@/stores/common'

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();
const commonStore = useCommonStore()

onBeforeMount(async () => {
    await lexiconStore.getEvents();
    await lexiconStore.getEventProperties();
    await lexiconStore.getUserProperties();

    await eventsStore.initPeriod();
});

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
