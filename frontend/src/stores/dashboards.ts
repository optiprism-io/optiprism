import { defineStore } from 'pinia';
import { Dashboard } from '@/api';
import { useCommonStore } from '@/stores/common';
import dashboardService from '@/api/services/dashboards.service';

type DashboardsStore = {
    dashboards: Dashboard[],
};

export const useDashboardsStore = defineStore('dashboards', {
    state: (): DashboardsStore => ({
        dashboards: [],
    }),
    actions: {
        async getDashboards() {
            const commonStore = useCommonStore();
            try {
                const res = await dashboardService.dashboardsList(commonStore.organizationId, commonStore.organizationId);
                if (res?.data?.data) {
                    this.dashboards = res.data.data
                }
            } catch (e) {
                console.error('error update event property');
            }
        },
    },
    getters: {},
});
