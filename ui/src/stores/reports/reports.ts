import { defineStore } from 'pinia'
import reportsService from '@/api/services/reports.service'
import { useCommonStore } from '@/stores/common'
import { Report } from '@/api'

type Reports = {
    list: Report[]
    loading: boolean
    reportId: number
}

export const useReportsStore = defineStore('reports', {
    state: (): Reports => ({
        list: [],
        loading: true,
        reportId: 0,
    }),
    getters: {},
    actions: {
        async getList() {
            this.loading = true
            const commonStore = useCommonStore()

            try {
                const res = await reportsService.reportsList(commonStore.organizationId, commonStore.projectId)

                if (res.data?.dashboards?.length) {
                    this.list = res.data.dashboards
                }
            } catch(e) {
                throw new Error('error reportsList');
            }

            this.loading = false
        }
    },
})
