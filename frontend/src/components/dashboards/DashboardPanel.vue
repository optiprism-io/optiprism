<template>
    <div class="dashboard-panel">
        <EventsViews
            :event-segmentation="eventSegmentation"
            :loading="loading"
            :only-view="true"
            @get-event-segmentation="getEventSegmentation"
        />
    </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from 'vue'
import { Report, EventChartType, ReportReportTypeEnum, DataTableResponse } from '@/api'
import reportsService from '@/api/services/reports.service'
import { useCommonStore } from '@/stores/common'

import EventsViews from '@/components/events/EventsViews.vue';

const commonStore = useCommonStore()

const props = defineProps<{
    report: Report
}>()

const loading = ref(false)
const eventSegmentation = ref<DataTableResponse>()

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