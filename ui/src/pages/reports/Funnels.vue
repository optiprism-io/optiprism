<template>
    <ToolsLayout>
        <template #title>
            {{ $t('funnels.untitledFunnel') }}
        </template>

        <UiCardContainer :title="$t('funnels.steps')">
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

        <UiCard :title="$t('funnels.userSegments')">
            <Segments />
        </UiCard>

        <UiCardContainer>
            <UiCardTitle>
                {{ $t('funnels.filters') }}

                <template
                    v-if="filterGroupsStore.filterGroups.length > 1"
                    #extra
                >
                    <div class="pf-l-flex">
                        <span class="pf-l-flex__item">match</span>
                        <UiSelectCondition
                            v-model="condition"
                            :items="conditionsItems"
                            :show-search="false"
                        >
                            <UiButton
                                class="pf-m-main pf-m-secondary pf-l-flex__item"
                                :is-link="true"
                            >
                                {{ $t(`filters.conditions.${condition}`) }}
                            </UiButton>
                        </UiSelectCondition>
                        <span class="pf-l-flex__item">groups</span>
                    </div>
                </template>
            </UiCardTitle>

            <UiCardBody>
                <FilterGroupsList />
            </UiCardBody>
        </UiCardContainer>

        <UiCard :title="$t('funnels.breakdowns')">
            <Breakdowns />
        </UiCard>

        <template #main>
            <FunnelsViews />
        </template>
    </ToolsLayout>
</template>

<script setup lang="ts">
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import TimeWindow from '@/components/funnels/time-window/TimeWindow.vue';
import UiCardTitle from '@/components/uikit/UiCard/UiCardTitle.vue';
import UiCardBody from '@/components/uikit/UiCard/UiCardBody.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import Breakdowns from '@/components/events/Breakdowns.vue';
import {computed, inject, onUnmounted} from 'vue';
import {useEventsStore} from '@/stores/eventSegmentation/events';
import ExcludeStepsList from '@/components/funnels/exclude/ExcludeStepsList.vue';
import HoldingConstantSelect from '@/components/funnels/holding/HoldingConstantSelect.vue';
import ExcludeStepSelect from '@/components/funnels/exclude/ExcludeStepSelect.vue';
import HoldingConstantList from '@/components/funnels/holding/HoldingConstantList.vue';
import StepsList from '@/components/funnels/steps/StepsList.vue';
import FilterGroupsList from '@/components/funnels/filters/FilterGroupsList.vue';
import Segments from '@/components/events/Segments/Segments.vue';
import {FilterCondition, filterConditions, useFilterGroupsStore} from '@/stores/funnels/filters';
import {UiSelectItemInterface} from '@/components/uikit/UiSelect/types';
import {UiSelectGeneric} from '@/components/uikit/UiSelect/UiSelectGeneric';
import {I18N} from '@/plugins/i18n';
import FunnelsViews from '@/components/funnels/view/FunnelsViews.vue';

const eventsStore = useEventsStore();
const filterGroupsStore = useFilterGroupsStore()
const { $t } = inject('i18n') as I18N

const UiSelectCondition = UiSelectGeneric<FilterCondition>()

onUnmounted(() => {
    eventsStore.$reset();
});

const condition = computed({
    get(): FilterCondition {
        return filterGroupsStore.condition;
    },
    set(value: FilterCondition) {
        filterGroupsStore.setCondition(value);
    }
})

const conditionsItems = computed<UiSelectItemInterface<FilterCondition>[]>(() => {
    return filterConditions.map(item => ({
        __type: 'item',
        id: item,
        label: $t(`filters.conditions.${item}`),
        value: item,
    }));
})
</script>
