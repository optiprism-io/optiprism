<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        Event Segmentation
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item pf-m-12-col-on-md pf-m-6-col-on-2xl">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>Events</p>
                </div>
                <div class="pf-c-card__body">
                    <Events />
                </div>
            </div>
        </div>
        <!-- TODO Segments implement later -->
        <!-- <div class="pf-l-grid__item pf-m-12-col-on-md pf-m-6-col-on-2xl">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>Segments</p>
                </div>
                <div class="pf-c-card__body">
                    <Breakdowns />
                </div>
            </div>
        </div> -->
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>Filters</p>
                </div>
                <div class="pf-c-card__body">
                    <Filters />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col pf-m-6-col-on-lg">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <div class="pf-c-card__title">
                    <p>Breakdowns</p>
                </div>
                <div class="pf-c-card__body">
                    <Breakdowns />
                </div>
            </div>
        </div>
        <div class="pf-l-grid__item pf-m-12-col">
            <EventsViews />
        </div>
    </div>
</template>

<script setup lang="ts">
import { onBeforeMount, onUnmounted } from "vue";
import Events from "@/components/events/Events/Events.vue";
import Breakdowns from "@/components/events/Breakdowns.vue";
import Filters from "@/components/events/Filters.vue";
import EventsViews from "@/components/events/EventsViews.vue";
import { useLexiconStore } from "@/stores/lexicon";
import { useEventsStore } from "@/stores/eventSegmentation/events";

const lexiconStore = useLexiconStore();
const eventsStore = useEventsStore();

onBeforeMount(async () => {
    await lexiconStore.getEvents();
    await lexiconStore.getEventProperties();
    await lexiconStore.getUserProperties();

    await eventsStore.initPeriod();
    await eventsStore.fetchEventSegmentationResult();
});

onUnmounted(() => {
    eventsStore.$reset();
});
</script>

<style scoped lang="scss">
.page-title {
    color: var(--op-base-color);
    font-size: 1.4rem;
    margin-bottom: .2rem;
}
</style>
