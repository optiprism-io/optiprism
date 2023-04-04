<template>
    <section class="pf-c-page__main-section">
        <ToolsLayout :col-lg="12">
            <template #title>
                {{ $t('users.title') }}
            </template>
            <UiCard
                class="pf-c-card pf-m-compact pf-u-h-100"
                :title="$t('events.segments.label')"
            >
                <Segments
                    @get-event-segmentation="updateData"
                />
            </UiCard>
            <template #main>
                <UiCardContainer class="pf-u-h-100">
                    <UiTable
                        :items="items"
                        :columns="columns"
                        :show-select-columns="true"
                        @on-action="onAction"
                    >
                        <template #before>
                            <UiToggleGroup
                                :items="itemsPeriod"
                                @select="onSelectPerion"
                            >
                                <template #after>
                                    <UiDatePickerWrappet
                                        :is-period-active="groupStore.isPeriodActive"
                                        :from="groupStore.period.from"
                                        :to="groupStore.period.to"
                                        :last="groupStore.period.last"
                                        :type="groupStore.period.type"
                                        @on-apply="onSelectData"
                                    />
                                </template>
                            </UiToggleGroup>
                        </template>
                    </UiTable>
                    <!-- TODO table data format -->
                    {{ groupStore.items }}
                </UiCardContainer>
            </template>
        </ToolsLayout>
    </section>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, onUnmounted } from 'vue';
import { useGroupStore } from '@/stores/group/group';
import { Action } from '@/components/uikit/UiTable/UiTable';
import { useSegmentsStore } from '@/stores/reports/segments';

import Segments from '@/components/events/Segments/Segments.vue';
import UiTable from '@/components/uikit/UiTable/UiTable.vue';
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import UiToggleGroup, { UiToggleGroupItem } from '@/components/uikit/UiToggleGroup.vue'
import UiDatePickerWrappet, { DataPickerPeriod } from '@/components/uikit/UiDatePickerWrappet.vue';
import { I18N } from '@/utils/i18n';

const i18n = inject('i18n') as I18N;
const groupStore = useGroupStore();
const segmentsStore = useSegmentsStore()

const itemsPeriod = computed(() => {
    return ['7', '30', '90'].map((key): UiToggleGroupItem => ({
        key,
        nameDisplay: key + i18n.$t('common.calendar.day_short'),
        value: key,
        selected: groupStore.controlsPeriod === key,
    }));
});

const items = computed(() => {
    return [];
});

const columns = computed(() => {
    return [];
});

const onAction = (payload: Action) => {
    // TODO action table
};

const updateData = () => {
    groupStore.getList();
};

const onSelectPerion = (payload: string) => {
    groupStore.controlsPeriod = payload;
    groupStore.period.type = 'notCustom';
    updateData();
};

const onSelectData = (payload: DataPickerPeriod, controlsPeriod: string) => {
    groupStore.controlsPeriod = controlsPeriod;
    groupStore.period = {
        ...groupStore.period,
        from: payload.from || '',
        to: payload.to || '',
        type: payload.type,
        last: payload.last,
    };
    updateData();
};

onMounted(() => {
    segmentsStore.$reset();
    updateData();
});

onUnmounted(() => {
    segmentsStore.$reset();
});
</script>

<style scoped lang="scss"></style>
