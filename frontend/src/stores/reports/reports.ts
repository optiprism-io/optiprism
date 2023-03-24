import { defineStore } from 'pinia'
import reportsService from '@/api/services/reports.service'
import { useCommonStore } from '@/stores/common'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useFunnelsStore } from '@/stores/funnels/funnels'
import { useStepsStore } from '@/stores/funnels/steps'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
import { useSegmentsStore } from '@/stores/reports/segments'
import {
    Report,
    ReportQuery,
    ReportType,
    EventRecordsListRequestTime,
    EventChartType,
    FunnelQueryChartTypeTypeEnum,
    TimeUnit,
    EventGroupedFilters,
    BreakdownByProperty,
    EventSegmentationSegment,
    FunnelQueryStepsInner,
    PropertyRef,
} from '@/api'

type Reports = {
    list: Report[]
    loading: boolean
    saveLoading: boolean
    reportId: number
    reportDump: string
    reportDumpType: ReportType
}

export const getReport = (type: ReportType) => {
    const eventsStore = useEventsStore()
    const funnelsStore = useFunnelsStore()
    const breakdownsStore = useBreakdownsStore()
    const filterGroupsStore = useFilterGroupsStore()
    const segmentsStore = useSegmentsStore()
    const stepsStore = useStepsStore()

    return {
        time: type === ReportType.EventSegmentation ? eventsStore.timeRequest as EventRecordsListRequestTime : funnelsStore.timeRequest as EventRecordsListRequestTime,
        group: eventsStore.group,
        intervalUnit: eventsStore.controlsGroupBy,
        chartType: type === ReportType.EventSegmentation ? eventsStore.chartType as EventChartType : {
            type: FunnelQueryChartTypeTypeEnum.Frequency,
            intervalUnit: TimeUnit.Day
        },
        analysis: { type: 'linear' },
        events: type === ReportType.EventSegmentation ? eventsStore.propsForEventSegmentationResult.events : [],
        filters: filterGroupsStore.filters as EventGroupedFilters,
        breakdowns: breakdownsStore.breakdownsItems as BreakdownByProperty[],
        segments: segmentsStore.segmentationItems as EventSegmentationSegment[],
        steps: stepsStore.getSteps as FunnelQueryStepsInner[],
        holdingConstants: stepsStore.getHoldingProperties as PropertyRef[],
        exclude: stepsStore.getExcluded,
    } as ReportQuery
}

export const useReportsStore = defineStore('reports', {
    state: (): Reports => ({
        list: [],
        loading: true,
        reportId: 0,
        saveLoading: false,
        reportDump: '',
        reportDumpType: ReportType.EventSegmentation,
    }),
    getters: {
        isChangedReport(): boolean {
            return !this.reportId || JSON.stringify(getReport(this.reportDumpType)) !== this.reportDump
        },
        activeReport(): null | Report {
            const report = this.list.find(item => item.id && Number(item.id) === Number(this.reportId))
            return report ?? null
        },
        reportsId(): number[] {
            return this.list.map(item => Number(item.id))
        },
    },
    actions: {
        updateDump(type: ReportType) {
            this.reportDumpType = type
            this.reportDump = JSON.stringify(getReport(type))
        },
        async getList() {
            const commonStore = useCommonStore()
            try {
                const res = await reportsService.reportsList(commonStore.organizationId, commonStore.projectId)
                if (res.data?.data) {
                    this.list = res.data.data
                }
            } catch (e) {
                throw new Error('error reportsList');
            }
        },
        async createReport(name: string, type: ReportType) {
            this.saveLoading = true
            const commonStore = useCommonStore()
            try {
                const res = await reportsService.createReport(commonStore.organizationId, commonStore.projectId, {
                    type,
                    name,
                    query: getReport(type)
                })
                if (res.data?.id) {
                    this.reportId = Number(res.data.id)
                }
            } catch (e) {
                throw new Error('error reportsList');
            }

            this.saveLoading = false
        },
        async editReport(name: string, type: ReportType) {
            this.saveLoading = true
            const commonStore = useCommonStore()
            await reportsService.updateReport(commonStore.organizationId, commonStore.projectId, Number(this.reportId), {
                name,
                query: getReport(type)
            })
            this.saveLoading = false
        },
        async deleteReport(reportId: number) {
            const commonStore = useCommonStore()
            await reportsService.deleteReport(commonStore.organizationId, commonStore.projectId, Number(reportId))
        },
    },
})
