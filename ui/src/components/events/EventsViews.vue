<template>
    <div class="pf-c-card pf-u-mb-md">
        <div class="pf-c-toolbar">
            <div class="pf-c-toolbar__content">
                <div class="pf-c-toolbar__content-section pf-m-nowrap">
                    <div class="pf-c-toolbar__item">
                        <UiSelect
                            :items="itemsGroupBy"
                            :text-button="selectedGroupByString"
                            :selections="[eventsStore.controlsGroupBy]"
                            @on-select="onSelectGroupBy"
                        />
                    </div>
                    <div class="pf-c-toolbar__item">
                        <UiToggleGroup
                            :items="itemsPeriod"
                            @select="onSelectPerion"
                        >
                            <template #after>
                                <UiDatePicker
                                    :value="calendarValue"
                                    :last-count="lastCount"
                                    :active-tab-controls="eventsStore.period.type"
                                    @on-apply="onApplyPeriod"
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
                    <div class="pf-c-toolbar__item">
                        <UiSelect
                            :items="compareToItems"
                            :text-button="textSelectCompairTo"
                            :selections="[eventsStore.compareTo]"
                            :clearable="true"
                            :full-text="true"
                            @on-clear="onSelectCompareTo('')"
                            @on-select="onSelectCompareTo"
                        />
                    </div>
                    <div class="pf-c-toolbar__item pf-u-ml-auto">
                        <UiLabelGroup
                            :label="'Chart type:'"
                        >
                            <template #content>
                                <UiToggleGroup
                                    :items="chartTypeItems"
                                    @select="onSelectChartType"
                                />
                            </template>
                        </UiLabelGroup>
                    </div>
                </div>
            </div>
        </div>
        <div class="pf-c-scroll-inner-wrapper pf-u-p-md">
            <component
                :is="chartEventsOptions.component"
                v-if="chartEventsOptions"
                :options="chartEventsOptions"
                :type="eventsStore.chartType"
                :loading="eventsStore.eventSegmentationLoading"
            />
            <div
                v-else
                class="content-info"
            >
                <div class="pf-u-display-flex content-info__icons pf-u-color-400">
                    <UiIcon
                        class="content-info__icon"
                        :icon="'fas fa-chart-pie'"
                    />
                    <UiIcon
                        class="content-info__icon"
                        :icon="'fas fa-chart-line'"
                    />
                </div>
                <div class="pf-c-card__title pf-u-text-align-center pf-u-font-size-lg pf-u-color-400">
                    Select at least one events by clicking
                    <b>+ add event</b>
                </div>
            </div>
        </div>
    </div>

    <div class="pf-c-card">
        <div class="pf-c-toolbar">
            <div class="pf-c-toolbar__content">
                <div class="pf-u-font-size-lg">
                    Breakdown Table
                </div>
            </div>
        </div>
        <div class="pf-c-scroll-inner-wrapper">
            <div
                v-if="eventsStore.eventSegmentationLoading"
                class="pf-u-min-height pf-u-display-flex pf-u-justify-content-center pf-u-align-items-center"
                style="--pf-u-min-height--MinHeight: 24ch;"
            >
                <UiSpinner :size="'xl'" />
            </div>
            <UiTable
                v-else
                :items="eventsStore.tableData"
                :columns="eventsStore.tableColumnsValues"
            />
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import UiSelect from "@/components/uikit/UiSelect.vue";
import { useEventsStore } from "@/stores/eventSegmentation/events";
import { groupByMap, periodMap } from "@/configs/events/controls";
import UiToggleGroup, {UiToggleGroupItem} from "@/components/uikit/UiToggleGroup.vue";
import UiIcon from "@/components/uikit/UiIcon.vue";
import UiDatePicker, { ApplyPayload } from "@/components/uikit/UiDatePicker.vue";
import { getStringDateByFormat } from "@/helpers/getStringDates";
import UiLabelGroup from "@/components/uikit/UiLabelGroup.vue";
import UiTable from "@/components/uikit/UiTable/UiTable.vue";
import ChartPie from "@/components/charts/ChartPie.vue";
import ChartLine from "@/components/charts/ChartLine.vue";
import ChartColumn from "@/components/charts/ChartColumn.vue";

const compareToMap = ['day', 'week', 'month', 'year'];

const chartTypeMap = [
    {
        value: 'line',
        icon: 'fas fa-chart-line',
    },
    {
        value: 'column',
        icon: 'fas fa-chart-bar',
    },
    {
        value: 'pie',
        icon: 'fas fa-chart-pie',
    },
];

const eventsStore = useEventsStore();

const chartEventsOptions = computed(() => {
    switch(eventsStore.chartType) {
        case 'line':
            return {
                data: eventsStore.lineChart,
                component: ChartLine,
                xField: 'date',
                yField: 'value',
                seriesField: 'category',
                xAxis: {
                    type: 'time',
                },
                yAxis: {
                    label: {
                        formatter: (v: number) => `${v}`.replace(/\d{1,3}(?=(\d{3})+$)/g, (s) => `${s},`),
                    },
                },
            };
        case 'pie':
            return {
                data: eventsStore.pieChart,
                component: ChartPie,
                appendPadding: 10,
                angleField: 'value',
                colorField: 'type',
                radius: 0.8,
                label: {
                    type: 'outer',
                    content: '{name} {percentage}',
                },
                interactions: [{ type: 'pie-legend-active' }, { type: 'element-active' }],
            };
        case 'column':
            return {
                data: eventsStore.pieChart,
                component: ChartColumn,
                xField: 'type',
                yField: 'value',
                seriesField: 'type',
                intervalPadding: 15,
                maxColumnWidth: 45,
            };
        default:
            return {};
    }
});

const compareToItems = computed(() => {
    return compareToMap.map(item => {
        return {
            key: item,
            nameDisplay: `Previous ${item}`,
            value: item,
        }
    })
})

const textSelectCompairTo = computed(() => {
    return eventsStore.compareTo ? `Compare to previous ${eventsStore.compareTo}` : 'Compare to Past'
})

const chartTypeItems = computed(() => {
    return chartTypeMap.map((item, i) => {
        return {
            key: `${item.value}-${i}`,
            iconAfter: item.icon,
            nameDisplay: '',
            selected: eventsStore.chartType === item.value,
            value: item.value,
        }
    })
})

const perios = computed(() => {
    return eventsStore.period;
});

const lastCount = computed(() => {
    return perios.value.last;
});

const calendarValue = computed(() => {
    return {
        from: perios.value.from,
        to: perios.value.to,
        multiple: false,
        dates: [],
    };
});

const calendarValueString = computed(() => {
    if (eventsStore.period.from && eventsStore.period.to && eventsStore.controlsPeriod === 'calendar') {
        switch(eventsStore.period.type) {
            case 'last':
                return `Last ${eventsStore.period.last} ${eventsStore.period.last === 1 ? 'day' : 'days'}`;
            case 'since':
                return `Since ${getStringDateByFormat(eventsStore.period.from, '%d %b, %Y')}`;
            case 'between':
                return `${getStringDateByFormat(eventsStore.period.from, '%d %b, %Y')} - ${getStringDateByFormat(eventsStore.period.to, '%d %b, %Y')}`;
            default:
                return '';
        }
    } else {
        return '';
    }
});

const itemsGroupBy = computed(() => {
    return groupByMap.map((key) => ({
        key,
        nameDisplay: key,
        value: key,
    }))
});

const itemsPeriod = computed(() => {
    const activeKey: string = eventsStore.controlsGroupBy;
    const config = periodMap.find(item => item.type === activeKey);

    if (config) {
        return config.items.map((key, i): UiToggleGroupItem => ({
            key,
            nameDisplay: key + config.text,
            value: key,
            selected: eventsStore.controlsGroupBy ? key === eventsStore.controlsPeriod : i === 0,
        }));
    } else {
        return [];
    }
})

const selectedGroupByString = computed(() => {
    const selectedGroupBy = itemsGroupBy.value.find(item => item.value === eventsStore.controlsGroupBy);

    return selectedGroupBy ? `Group by ${selectedGroupBy.nameDisplay}` : '';
});

const onSelectGroupBy = (payload: string) => {
    eventsStore.initPeriod();
    eventsStore.controlsGroupBy = payload;
    eventsStore.controlsPeriod = itemsPeriod.value[itemsPeriod.value.length - 1].value;
    updateEventSegmentationData();
};

const onSelectPerion = (payload: string) => {
    eventsStore.controlsPeriod = payload;
    eventsStore.initPeriod();
    updateEventSegmentationData();
};

const onApplyPeriod = (payload: ApplyPayload): void => {
    eventsStore.controlsPeriod = 'calendar';
    eventsStore.period = {
        ...eventsStore.period,
        from: payload.value.from || '',
        to: payload.value.to || '',
        type: payload.type,
        last: payload.last,
    };
    updateEventSegmentationData();
};

const onSelectCompareTo = (payload: string): void => {
    eventsStore.compareTo = payload
}

const onSelectChartType = (payload: string): void => {
    eventsStore.chartType = payload;
}

const updateEventSegmentationData = async () => {
    await eventsStore.fetchEventSegmentationResult()
}
</script>

<style lang="scss" scoped>
.content-info {
    height: 320px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;

    &__icons {
        margin-bottom: 25px;
        font-size: 68px;
    }

    &__icon {
        margin: 0 15px;
    }
}
</style>