<template>
    <TemplateReport>
        <template #content>
            <GridContainer>
                <GridItem :col-lg="6">
                    <UiCard :title="$t('events.events')">
                        <Events @get-event-segmentation="getEventSegmentation" />
                    </UiCard>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCard :title="$t('events.segments.label')">
                        <Segments />
                    </UiCard>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCardContainer>
                        <FilterReports />
                    </UiCardContainer>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCard :title="$t('events.breakdowns')">
                        <Breakdowns />
                    </UiCard>
                </GridItem>
            </GridContainer>
        </template>
        <template #views>
            <EventsViews
                :event-segmentation="eventSegmentation"
                :loading="eventSegmentationLoading"
                @get-event-segmentation="getEventSegmentation"
            />
        </template>
    </TemplateReport>
</template>

<script setup lang="ts">
import { onUnmounted, watch, ref } from 'vue';
import Events from '@/components/events/Events/Events.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import EventsViews from '@/components/events/EventsViews.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue'
import FilterReports from '@/components/events/FiltersReports.vue'
import TemplateReport from '@/components/events/TemplateReport.vue'
import GridContainer from '@/components/grid/GridContainer.vue';
import GridItem from '@/components/grid/GridItem.vue';
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
        throw new Error('error event segmentation')
    }
    eventSegmentationLoading.value = false
}

watch(() => eventsStore.events, () => getEventSegmentation())
</script>

<style scoped lang="scss">
.page-title {
    color: var(--op-base-color);
    font-size: 1.4rem;
    margin-bottom: .2rem;
}
</style>
