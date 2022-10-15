<template>
    <div class="dashboard-panel">
        <EventsViews
            class="dashboard-panel__views"
            :event-segmentation="eventSegmentation"
            :loading="loading"
            :chart-type="reportChartType"
            :only-view="true"
            :lite-chart="true"
            :height-chart="240"
        />
    </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from 'vue'
import { Report, EventChartType, DataTableResponse } from '@/api'
import reportsService from '@/api/services/reports.service'
import { ChartType } from '@/stores/eventSegmentation/events';
import { useCommonStore } from '@/stores/common'

import EventsViews from '@/components/events/EventsViews.vue';

const commonStore = useCommonStore()

const props = defineProps<{
    report: Report
}>()

const loading = ref(false)
const eventSegmentation = ref<DataTableResponse>()
const reportChartType = computed(() => props.report?.report?.chartType as ChartType || 'line')

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
        } catch (e) {
            console.log(e);
        }

    }
    loading.value = false
}

onMounted(() => {
    getEventSegmentation()
})
</script>

<style lang="scss">
</style>