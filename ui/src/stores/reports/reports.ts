import { defineStore } from 'pinia'
import reportsService from '@/api/services/reports.service'
import { useCommonStore } from '@/stores/common'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useFunnelsStore } from '@/stores/funnels/funnels'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
import { useSegmentsStore } from '@/stores/reports/segments'
import {
    Report,
    ReportReport,
    ReportReportTypeEnum,
    FunnelQueryChartType,
} from '@/api'

type Reports = {
    list: Report[]
    loading: boolean
    saveLoading: boolean
    reportId: number
}

const getReport = (type: ReportReportTypeEnum): ReportReport => {
    const eventsStore = useEventsStore()
    const funnelsStore = useFunnelsStore()
    const breakdownsStore = useBreakdownsStore()
    const filterGroupsStore = useFilterGroupsStore()
    const segmentsStore = useSegmentsStore()

    return {
        type,
        time: type === ReportReportTypeEnum.EventSegmentation ? eventsStore.timeRequest : funnelsStore.timeRequest,
        group: eventsStore.group,
        intervalUnit: eventsStore.controlsGroupBy,
        chartType: eventsStore.chartType as FunnelQueryChartType,
        analysis: {
            type: 'linear',
        },
        events: type === ReportReportTypeEnum.EventSegmentation ? eventsStore.propsForEventSegmentationResult.events : [],
        filters: filterGroupsStore.filters,
        breakdowns: breakdownsStore.breakdownsItems,
        segments: segmentsStore.segmentationItems,

        // TODO funnels
    }
}

export const useReportsStore = defineStore('reports', {
    state: (): Reports => ({
        list: [],
        loading: true,
        reportId: 0,
        saveLoading: false,
    }),
    getters: {
        activeReport(): null | Report {
            const report = this.list.find(item => item.id && Number(item.id) === Number(this.reportId))

            return report ?? null
        },
        reportsId(): number[] {
            return this.list.map(item => Number(item.id))
        },
    },
    actions: {
        async getList() {
            const commonStore = useCommonStore()

            try {
                const res = await reportsService.reportsList(commonStore.organizationId, commonStore.projectId)

                if (res.data?.dashboards?.length) {
                    this.list = res.data.dashboards
                }
            } catch(e) {
                throw new Error('error reportsList');
            }
        },
        async createReport(name: string, type: ReportReportTypeEnum) {
            this.saveLoading = true
            const commonStore = useCommonStore()

            try {
                const res = await reportsService.createReport(commonStore.organizationId, commonStore.projectId, {
                    name,
                    report: getReport(type)
                })

                if (res.data?.id) {
                    this.reportId = Number(res.data.id)
                }
            } catch(e) {
                throw new Error('error reportsList');
            }

            this.saveLoading = false
        },
        async editReport(name: string, type: ReportReportTypeEnum) {
            this.saveLoading = true
            const commonStore = useCommonStore()

            await reportsService.updateReport(commonStore.organizationId, commonStore.projectId, Number(this.reportId), {
                name,
                report: getReport(type)
            })
            this.saveLoading = false
        }
    },
})
