<template>
    <section class="pf-c-page__main-section">
        <ToolsLayout :col-lg="12">
            <template #title>
                {{ $t('users.title') }}
            </template>
            <UiCard
                class="pf-c-card pf-m-compact pf-u-h-100"
                :title="$t('events.segments.segment')"
            >
                <Segments
                    :is-one="true"
                    :hide-add-segment-button="true"
                    @get-event-segmentation="getEventSegmentationDebounce"
                />
            </UiCard>
            <template #main>
                <UiCardContainer class="pf-u-h-100">
                    <UiTable
                        :items="items"
                        :columns="columns"
                        :show-select-columns="true"
                        :is-loading="groupStore.loading"
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
                </UiCardContainer>
            </template>
        </ToolsLayout>
        <PropertiesManagementPopup
            v-if="groupStore.propertyPopup"
            :item="selectedItes"
            @apply="onClosePropertyPopup"
        />
    </section>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, onUnmounted, ref } from 'vue';
import { Row, Action } from '@/components/uikit/UiTable/UiTable';
import { useGroupStore } from '@/stores/group/group';
import { useSegmentsStore } from '@/stores/reports/segments';
import { GroupRecord } from '@/api';
import { I18N } from '@/utils/i18n';

import Segments from '@/components/events/Segments/Segments.vue';
import ToolsLayout from '@/layout/tools/ToolsLayout.vue';
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue';
import UiCard from '@/components/uikit/UiCard/UiCard.vue';
import UiTable from '@/components/uikit/UiTable/UiTable.vue';
import UiToggleGroup, { UiToggleGroupItem } from '@/components/uikit/UiToggleGroup.vue';
import UiDatePickerWrappet, { DataPickerPeriod } from '@/components/uikit/UiDatePickerWrappet.vue';
import UiCellToolMenu from '@/components/uikit/cells/UiCellToolMenu.vue';
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue';
import PropertiesManagementPopup from '@/components/groups/PropertiesManagementPopup.vue';
import { shortPeriodDays } from '@/components/uikit/UiCalendar/UiCalendar.config';
import { debounce } from 'lodash';

const i18n = inject('i18n') as I18N;
const groupStore = useGroupStore();
const segmentsStore = useSegmentsStore();
const selectedItes = ref<GroupRecord | null>(null);

const itemsPeriod = computed(() => {
    return shortPeriodDays.map((key): UiToggleGroupItem => ({
        key,
        nameDisplay: key + i18n.$t('common.calendar.dayShort'),
        value: key,
        selected: groupStore.controlsPeriod === key,
    }));
});

const columnsPropertiesKeys = computed(() => {
    const properties = groupStore.items.map(item => {
        return Object.keys(item.properties);
    });
    return [...new Set(properties.flat())];
});

const columns = computed(() => {
    return groupStore.items.length ? ['id', ...columnsPropertiesKeys.value, 'action'].map(key => {
        const isAction = key === 'action';
        return {
            value: key,
            title: isAction ? '' : key === 'id' ? i18n.$t(`groups.columns.${key}`) : key,
            default: isAction,
            type: isAction? 'action' : '',
            fitContent: key === 'id',
        };
    }) : [];
});

const items = computed(() => {
    return groupStore.items.map((item: GroupRecord): Row => {
        return [
            {
                key: 'id',
                value: 'id',
                title: item.id,
                component: UiTablePressedCell,
                action: {
                    type: item.id,
                    name: item.group,
                }
            },
            ...columnsPropertiesKeys.value.map(key => {
                const value = item.properties[key] ?? '';
                return {
                    key,
                    value,
                    title: value,
                    nowrap: true,
                };
            }),
            {
                title: 'action',
                key: 'action',
                value: item.id,
                component: UiCellToolMenu,
                items: [
                    {
                        label: i18n.$t('common.edit'),
                        value: 'edit',
                    },
                ],
                type: 'action'
            },
        ]
    })
});

const getEventSegmentationDebounce = debounce(() => {
    updateData()
}, 2000);

const onAction = (payload: Action) => {
    const item = groupStore.items.find(item => item.id === payload.type);
    if (item) {
        selectedItes.value = item;
    }
    groupStore.propertyPopup = true;
};

const updateData = () => {
    groupStore.getList();
};

const onSelectPerion = (payload: string) => {
    groupStore.controlsPeriod = payload;
    groupStore.period.type = 'notCustom';
    updateData();
};

const onClosePropertyPopup = () => {
    selectedItes.value = null;
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
    segmentsStore.segments.push({
        name: '',
        conditions: [{
            filters: []
        }],
    });
    updateData();
});

onUnmounted(() => {
    segmentsStore.$reset();
});
</script>

<style scoped lang="scss"></style>
