<template>
    <div class="dashboard-panel">
        <div class="dashboard-panel__name">
            <router-link
                v-if="reportLink"
                :to="reportLink"
            >
                {{ report?.name }}
            </router-link>
        </div>
        <EventsViews
            v-if="reportType === ReportType.EventSegmentation"
            class="dashboard-panel__views"
            :event-segmentation="eventSegmentation"
            :loading="loading"
            :chart-type="reportChartType"
            :only-view="true"
            :lite-chart="true"
            :height-chart="props.heightChart || 240"
        />
        <FunnelsChart
            v-else
            :lite-chart="true"
            :reports="funnelsReport"
            :steps="steps"
            :height="funnelsChartHeight"
            :min-width-step="100"
        />
    </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from 'vue'
import {
    Report,
    EventChartType,
    DataTableResponse,
    ReportType,
    DataTableResponseColumnsInner,
    FunnelQuery,
    EventSegmentation as EventSegmentationType,
} from '@/api';

import reportsService from '@/api/services/reports.service'
import { ChartType } from '@/stores/eventSegmentation/events';
import { useCommonStore } from '@/stores/common'
import { useReportsStore } from '@/stores/reports/reports';
import dataService from '@/api/services/datas.service'
import schemaReports from '@/api/services/reports.service';
import { Step } from '@/types/steps'
import { mapReportToSteps } from '@/utils/reportsMappings'
import { pagesMap } from '@/router'

import EventsViews from '@/components/events/EventsViews.vue';
import FunnelsChart from '@/components/funnels/view/FunnelsChart.vue';

const commonStore = useCommonStore()
const reportsStore = useReportsStore()

const props = defineProps<{
    report?: Report
    reportId?: number
    heightChart?: number
}>()

const activeReport = ref<Report>()
const loading = ref(false)
const eventSegmentation = ref<DataTableResponse>()
const funnelsReport = ref<DataTableResponseColumnsInner[]>()
const steps = ref<Step[]>()

const funnelsChartHeight = computed(() => props.heightChart ? props.heightChart - 40 : 190);
const report = computed(() => props.report || activeReport.value);
const query = computed(() => report.value?.query);
const reportChartType = computed(() => report.value?.query?.chartType as ChartType ?? 'line')
const reportType = computed(() => report.value?.type ?? ReportType.EventSegmentation)
const reportLink = computed(() => {
    if (report.value) {
        return {
            name: report.value?.type === ReportType.EventSegmentation ? pagesMap.reportsEventSegmentation.name : pagesMap.funnels.name,
            params: {
                id: report.value?.id
            }
        }
    }
    return null;
})

const getEventSegmentation = async () => {
    loading.value = true
    if (query.value) {
        try {
            const res = await reportsService.eventSegmentation(commonStore.organizationId, commonStore.projectId, {
                ...query.value as EventSegmentationType,
                chartType: query.value.chartType as EventChartType,
            })
            if (res) {
                eventSegmentation.value = res.data as DataTableResponse
            }
        } catch (error) {
            throw Error(JSON.stringify(error))
        }
    }
    loading.value = false
}

const getFunnelsReport = async () => {
    loading.value = true
    if (query.value) {
        try {
            const query = report.value?.query as FunnelQuery;
            const res = await dataService.funnelQuery(commonStore.organizationId, commonStore.projectId, query)

            if (res?.data?.columns) {
                funnelsReport.value = res.data.columns as DataTableResponseColumnsInner[]
            }
        } catch (error) {
            throw Error(JSON.stringify(error))
        }
    }
    loading.value = false
}

const getReport = async () => {
    loading.value = true;
    if (props.reportId) {
        try {
            let report = reportsStore.list.find(item => item.id === props.reportId);
            if (!report) {
                const res = await schemaReports.getReport(commonStore.organizationId, commonStore.projectId, props.reportId);
                if (res?.data) {
                    report = res.data;
                }
            }
            activeReport.value = report
        } catch (error) {
            throw Error(JSON.stringify(error));
        }
    }
    loading.value = false;
};

const updateState = async () => {
    if (!props.report && props.reportId) {
        await getReport();
    }
    if (reportType.value === ReportType.EventSegmentation) {
        getEventSegmentation()
    } else if (reportType.value === ReportType.Funnel) {
        getFunnelsReport()
        const query = report.value?.query as FunnelQuery;
        if (query.steps) {
            steps.value = await mapReportToSteps(query.steps)
        }
    }
}

onMounted( () => {
    updateState()
})

watch(() => props.reportId, (id, oldValue) => {
    if (id && id !== oldValue) {
        updateState()
    }
})
</script>

<style lang="scss">
.dashboard-panel {
    margin-top: -24px;
    overflow: hidden;
    height: calc(100% + 24px);
    &__name {
        font-size: 16px;
    }
    .chart-wrapper__container {
        height: 100%;
    }
}
</style>