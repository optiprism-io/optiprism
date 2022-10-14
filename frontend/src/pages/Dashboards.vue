<template>
    <div class="dashboards pf-u-p-md">
        <div
            v-if="dashboards.length"
            class="pf-u-mb-md"
        >
            <UiSelect
                class="pf-u-w-25-on-md"
                :items="selectDashboards"
                :text-button="selectDashboardsText"
                :selections="selections"
                @on-select="onSelectDashboard"
            />
        </div>
        <GridContainer v-if="activeDashboardId">
            <template
                v-for="(item, i) in activeDashboard?.rows"
                :key="i"
            >
                <GridItem
                    v-for="panel in item?.panels"
                    :key="panel.reportId"
                    :col-lg="panel.span ?? 6"
                >
                    <UiCard
                        v-if="panel?.report"
                        :title="panel?.report?.name"
                        :link="{
                            name: panel.report?.report?.type === ReportReportTypeEnum.EventSegmentation ? pagesMap.reportsEventSegmentation.name : pagesMap.funnels.name,
                            params: {
                                id: panel?.report?.id
                            }
                        }"
                    >
                        <DashboardPanel
                            :report="panel.report"
                        />
                    </UiCard>
                </GridItem>
            </template>
        </GridContainer>
    </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import dashboardService from '@/api/services/dashboards.service'
import { Dashboard, ReportReportTypeEnum } from '@/api'
import { pagesMap } from '@/router'

import { useCommonStore } from '@/stores/common'
import usei18n from '@/hooks/useI18n'

import UiSelect from '@/components/uikit/UiSelect.vue'
import GridContainer from '@/components/grid/GridContainer.vue'
import GridItem from '@/components/grid/GridItem.vue'
import UiCard from '@/components/uikit/UiCard/UiCard.vue'
import DashboardPanel from '@/components/dashboards/DashboardPanel.vue'

const { t } = usei18n()
const commonStore = useCommonStore()

const dashboards = ref<Dashboard[]>([])
const activeDashboardId = ref<number | null>(null)

const selections = computed(() => activeDashboardId.value ? [activeDashboardId.value] : [])
const activeDashboard = computed(() => {
    return dashboards.value.find(item => Number(item.id) === activeDashboardId.value) ?? null
})

const selectDashboards = computed(() => {
    return dashboards.value.map(item => {
        const id = Number(item.id)
        return {
            value: id,
            key: id,
            nameDisplay: item.name || '',
        }
    })
})

const selectDashboardsText = computed(() => {
    return activeDashboard.value ? activeDashboard.value.name : t('dashboards.selectDashboard')
})

const onSelectDashboard = (id: number | string) => {
    activeDashboardId.value = Number(id);
}

onMounted(async() => {
    const res = await dashboardService.dashboardsList(commonStore.organizationId, commonStore.organizationId)

    if (res?.data?.dashboards) {
        dashboards.value = res.data.dashboards

        if (res.data.dashboards[0]?.id) {
            onSelectDashboard(res.data.dashboards[0].id)
        }
    }
})
</script>