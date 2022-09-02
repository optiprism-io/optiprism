<template>
    <ToolsLayout>
        <template #title>
            <div class="pf-u-display-flex pf-u-justify-content-space-between pf-u-align-items-center">
                <span>{{ $t('events.event_segmentation') }}</span>
                <UiSwitch
                    class="pf-u-ml-auto"
                    :value="commonStore.syncReports"
                    :label="$t('reports.sync')"
                    @input="(value: boolean) => commonStore.syncReports = value"
                />
            </div>
        </template>

        <UiCard :title="$t('events.events')">
            <Events @get-event-segmentation="getEventSegmentation" />
        </UiCard>

        <UiCard :title="$t('events.segments.label')">
            <Segments />
        </UiCard>

        <UiCardContainer>
            <FilterReports />
        </UiCardContainer>

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
import { onUnmounted, ref } from 'vue';
import Events from '@/components/events/Events/Events.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import EventsViews from '@/components/events/EventsViews.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue'
import FilterReports from '@/components/events/FiltersReports.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import reportsService from '@/api/services/reports.service'
import { DataTableResponse } from '@/api'
import { eventsToFunnels } from '@/utils/reportsMappings'

import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useCommonStore } from '@/stores/common'
import { useSegmentsStore } from '@/stores/reports/segments'

const eventsStore = useEventsStore();
const filterGroupsStore = useFilterGroupsStore()
const commonStore = useCommonStore()
const segmentsStore = useSegmentsStore()

const eventSegmentationLoading = ref(false)
const eventSegmentation = ref<DataTableResponse>()

onUnmounted(() => {
    if (commonStore.syncReports) {
        eventsToFunnels()
    } else {
        eventsStore.$reset()
        filterGroupsStore.$reset()
        segmentsStore.$reset()
    }
})

const getEventSegmentation = async () => {
    eventSegmentationLoading.value = true
    try {
        const res = await reportsService.eventSegmentation(commonStore.organizationId, commonStore.projectId,  eventsStore.propsForEventSegmentationResult)

        if (res) {
            eventSegmentation.value = res.data as DataTableResponse
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
