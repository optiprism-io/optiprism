<template>
    <div class="dashboard-panel">
        <EventsViews
            v-if="reportType === ReportReportTypeEnum.EventSegmentation"
            class="dashboard-panel__views"
            :event-segmentation="eventSegmentation"
            :loading="loading"
            :chart-type="reportChartType"
            :only-view="true"
            :lite-chart="true"
            :height-chart="240"
        />
        <FunnelsChart
            v-else
            :lite-chart="true"
            :reports="funnelsReport"
            :steps="steps"
            :height="190"
            :min-width-step="100"
        />
    </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from 'vue'
import { Report, EventChartType, DataTableResponse, ReportReportTypeEnum, DataTableResponseColumnsInner } from '@/api'
import reportsService from '@/api/services/reports.service'
import { ChartType } from '@/stores/eventSegmentation/events';
import { useCommonStore } from '@/stores/common'
import dataService from '@/api/services/datas.service'
import { Step } from '@/types/steps'
import { mapReportToSteps } from '@/utils/reportsMappings'

import EventsViews from '@/components/events/EventsViews.vue';
import FunnelsChart from '@/components/funnels/view/FunnelsChart.vue';

const commonStore = useCommonStore()

const props = defineProps<{
    report: Report
}>()

const loading = ref(false)
const eventSegmentation = ref<DataTableResponse>()
const funnelsReport = ref<DataTableResponseColumnsInner[]>()
const steps = ref<Step[]>()
const reportChartType = computed(() => props.report?.report?.chartType as ChartType ?? 'line')
const reportType = computed(() => props.report?.report?.type ?? 'eventSegmentation')

const getEventSegmentation = async () => {
    loading.value = true
    if (props.report?.report) {
        try {
            const res = await reportsService.eventSegmentation(commonStore.organizationId, commonStore.projectId, {
                ...props.report.report,
                chartType: props.report.report.chartType as EventChartType,
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
    if (props.report?.report) {
        try {
            const res = await dataService.funnelQuery(commonStore.organizationId, commonStore.projectId, props.report.report)

            if (res?.data?.columns) {
                funnelsReport.value = res.data.columns as DataTableResponseColumnsInner[]
            }
        } catch (error) {
            throw Error(JSON.stringify(error))
        }
    }
    loading.value = false
}

onMounted(async () => {
    if (reportType.value === ReportReportTypeEnum.EventSegmentation) {
        getEventSegmentation()
    } else {
        getFunnelsReport()

        if (props.report?.report?.steps) {
            steps.value = await mapReportToSteps(props.report?.report?.steps)
        }
    }
})
</script>

<style lang="scss">
</style>