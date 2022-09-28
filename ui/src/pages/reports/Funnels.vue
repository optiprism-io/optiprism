<template>
    <TemplateReport>
        <template #content>
            <GridContainer>
                <GridItem :col-lg="6">
                    <UiCardContainer :title="$t('funnels.steps.title')">
                        <UiCardTitle>
                            {{ $t('funnels.steps.title') }}
                        </UiCardTitle>
                        <UiCardBody>
                            <StepsList />
                        </UiCardBody>
                        <UiCardTitle>
                            {{ $t('criteria.label') }}
                        </UiCardTitle>
                        <UiCardBody class="pf-l-flex pf-m-column">
                            <TimeWindow />
                            <HoldingConstantList />
                            <ExcludeStepsList />
                        </UiCardBody>
                        <UiCardBody class="pf-l-flex">
                            <ExcludeStepSelect />
                            <HoldingConstantSelect />
                        </UiCardBody>
                    </UiCardContainer>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCard :title="$t('funnels.userSegments')">
                        <Segments />
                    </UiCard>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCardContainer>
                        <FilterReports />
                    </UiCardContainer>
                </GridItem>
                <GridItem :col-lg="6">
                    <UiCard :title="$t('funnels.breakdowns')">
                        <Breakdowns />
                    </UiCard>
                </GridItem>
            </GridContainer>
        </template>
        <template #views>
            <FunnelsViews />
        </template>
    </TemplateReport>
</template>

<script setup lang="ts">
import { onUnmounted } from 'vue'
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import TimeWindow from '@/components/funnels/time-window/TimeWindow.vue';
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue';
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import ExcludeStepsList from '@/components/funnels/exclude/ExcludeStepsList.vue';
import HoldingConstantSelect from '@/components/funnels/holding/HoldingConstantSelect.vue';
import ExcludeStepSelect from '@/components/funnels/exclude/ExcludeStepSelect.vue';
import HoldingConstantList from '@/components/funnels/holding/HoldingConstantList.vue';
import StepsList from '@/components/funnels/steps/StepsList.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import FunnelsViews from '@/components/funnels/view/FunnelsViews.vue';
import FilterReports from '@/components/events/FiltersReports.vue'
import TemplateReport from '@/components/events/TemplateReport.vue'
import GridContainer from '@/components/grid/GridContainer.vue'
import GridItem from '@/components/grid/GridItem.vue'
import { funnelsToEvents } from '@/utils/reportsMappings'

import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useSegmentsStore } from '@/stores/reports/segments'
import { useCommonStore } from '@/stores/common'

const eventsStore = useEventsStore()
const filterGroupsStore = useFilterGroupsStore()
const segmentsStore = useSegmentsStore()
const commonStore = useCommonStore()

onUnmounted(() => {
    if (commonStore.syncReports) {
        funnelsToEvents()
    } else {
        eventsStore.$reset()
        filterGroupsStore.$reset()
        segmentsStore.$reset()
    }
})
</script>
