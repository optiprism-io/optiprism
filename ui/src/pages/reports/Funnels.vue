<template>
    <ToolsLayout>
        <template #title>
            {{ $t('funnels.untitledFunnel') }}
        </template>

        <UiCardContainer :title="$t('funnels.steps')">
            <UiCardTitle>
                {{ $t('funnels.steps') }}
            </UiCardTitle>

            <UiCardBody>
                <Events
                    identifier="numeric"
                    :create-with-query="false"
                >
                    <template #new>
                        <UiButton
                            class="pf-m-main"
                            :is-link="true"
                            :before-icon="'fas fa-plus'"
                        >
                            {{ $t('common.add_step') }}
                        </UiButton>
                    </template>
                </Events>
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

        <UiCard :title="$t('funnels.userSegments')">
            {{ $t('funnels.userSegments') }}
        </UiCard>

        <UiCard :title="$t('funnels.filters')">
            {{ $t('funnels.filters') }}
        </UiCard>

        <UiCard :title="$t('funnels.breakdowns')">
            <Breakdowns />
        </UiCard>
    </ToolsLayout>
</template>

<script setup lang="ts">
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import TimeWindow from '@/components/funnels/steps/TimeWindow.vue';
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue';
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import Events from '@/components/events/Events/Events.vue';
import {onUnmounted} from 'vue';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import ExcludeStepsList from '@/components/funnels/exclude/ExcludeStepsList.vue';
import HoldingConstantSelect from '@/components/funnels/holding/HoldingConstantSelect.vue';
import ExcludeStepSelect from '@/components/funnels/exclude/ExcludeStepSelect.vue';
import HoldingConstantList from '@/components/funnels/holding/HoldingConstantList.vue';

const eventsStore = useEventsStore();

onUnmounted(() => {
    eventsStore.$reset();
});
</script>
