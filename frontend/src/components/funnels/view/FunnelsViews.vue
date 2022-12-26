<template>
    <div class="pf-c-card pf-u-mb-md">
        <div class="pf-c-toolbar">
            <div class="pf-c-toolbar__content">
                <div class="pf-c-toolbar__content-section pf-m-nowrap">
                    <div class="pf-c-toolbar__item">
                        <UiToggleGroup
                            :items="itemsPeriod"
                            @select="selectPeriod"
                        >
                            <template #after>
                                <UiDatePicker
                                    :value="calendarValue"
                                    :last-count="lastCount"
                                    :active-tab-controls="funnelsStore.period.type"
                                    @on-apply="applyPeriod"
                                >
                                    <template #action>
                                        <button
                                            class="pf-c-toggle-group__button"
                                            :class="{
                                                'pf-m-selected': calendarValueString,
                                            }"
                                            type="button"
                                        >
                                            <div class="pf-u-display-flex pf-u-align-items-center">
                                                <UiIcon :icon="'far fa-calendar-alt'" />
                                                &nbsp;
                                                {{ calendarValueString }}
                                            </div>
                                        </button>
                                    </template>
                                </UiDatePicker>
                            </template>
                        </UiToggleGroup>
                    </div>

                    <div class="pf-c-toolbar__item pf-u-ml-auto">
                        <UiDropdown
                            :items="items"
                            :text-button="itemText"
                            @select-value="selectItem"
                        />
                    </div>
                </div>
            </div>
        </div>

        <DataEmptyPlaceholder v-if="funnelsStore.reports.length === 0">
            {{ $t('funnels.view.selectToStart') }}
        </DataEmptyPlaceholder>
        <DataLoader v-else-if="funnelsStore.loading" />
        <template v-else>
            <FunnelsChart />
            <FunnelsTable class="pf-u-mt-xl" />
        </template>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject, ref, watch } from 'vue'
import UiDatePicker from '@/components/uikit/UiDatePicker.vue'
import {periodMap} from '@/configs/events/controls';
import {UiToggleGroupItem} from '@/components/uikit/UiToggleGroup.vue';
import {useFunnelsStore} from '@/stores/funnels/funnels';
import {getStringDateByFormat} from '@/helpers/getStringDates';
import {ApplyPayload} from '@/components/uikit/UiCalendar/UiCalendar';
import FunnelsChart from '@/components/funnels/view/FunnelsChart.vue';
import {I18N} from '@/utils/i18n';
import {UiDropdownItem} from '@/components/uikit/UiDropdown.vue';
import FunnelsTable from '@/components/funnels/view/FunnelsTable.vue';
import {useStepsStore} from '@/stores/funnels/steps';
import DataEmptyPlaceholder from '@/components/common/data/DataEmptyPlaceholder.vue';
import DataLoader from '@/components/common/data/DataLoader.vue';

const { $t } = inject('i18n') as I18N

const items = [
    {
        key: 0,
        value: 0,
        nameDisplay: $t('funnels.view.funnelSteps')
    }
];

const item = ref<string | number>(0)
const itemText = computed(() => items.find(c => c.key === item.value)?.nameDisplay ?? '')

const selectItem = (value: UiDropdownItem<string>) => {
    item.value = value.key;
}

const funnelsStore = useFunnelsStore()
const stepsStore = useStepsStore()
const loading = ref(false)

const itemsPeriod = computed(() => {
    const config = periodMap.find(item => item.type === 'day');

    return config?.items.map((key, i): UiToggleGroupItem => ({
        key,
        nameDisplay: key + config.text,
        value: key,
        selected: key === funnelsStore.controlsPeriod,
    })) ?? []
})

const period = computed(() => {
    return funnelsStore.period;
});

const calendarValue = computed(() => {
    return {
        from: period.value.from,
        to: period.value.to,
        multiple: false,
        dates: [],
    };
});

const lastCount = computed(() => {
    return period.value.last;
});

const calendarValueString = computed(() => {
    if (funnelsStore.period.from && funnelsStore.period.to && funnelsStore.controlsPeriod === 'calendar') {
        switch(funnelsStore.period.type) {
            case 'last':
                return `Last ${funnelsStore.period.last} ${funnelsStore.period.last === 1 ? 'day' : 'days'}`;
            case 'since':
                return `Since ${getStringDateByFormat(funnelsStore.period.from, '%d %b, %Y')}`;
            case 'between':
                return `${getStringDateByFormat(funnelsStore.period.from, '%d %b, %Y')} - ${getStringDateByFormat(funnelsStore.period.to, '%d %b, %Y')}`;
            default:
                return '';
        }
    } else {
        return '';
    }
});

const selectPeriod = (payload: string): void => {
    funnelsStore.setControlsPeriod(payload);
    funnelsStore.initPeriod();
};

const applyPeriod = (payload: ApplyPayload): void => {
    funnelsStore.setControlsPeriod('calendar');
    funnelsStore.setPeriod({
        ...funnelsStore.period,
        from: payload.value.from || '',
        to: payload.value.to || '',
        type: payload.type,
        last: payload.last,
    })
};

watch(() => stepsStore.steps.length, funnelsStore.getReports)
</script>
