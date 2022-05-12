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
                <Steps />
            </UiCardBody>

            <UiCardTitle>
                {{ $t('criteria.label') }}
            </UiCardTitle>

            <UiCardBody>
                <Criteria />
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
import Steps from '@/components/funnels/steps/Steps.vue';
import Criteria from '@/components/funnels/steps/Criteria.vue';
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue';
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
</script>
