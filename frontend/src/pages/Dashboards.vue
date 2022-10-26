<template>
    <div class="dashboards pf-u-p-md">
        <div class="pf-u-mb-md pf-u-display-flex">
            <UiSelect
                v-if="dashboards.length"
                class="pf-u-w-25-on-md pf-u-mr-md"
                :items="selectDashboards"
                :text-button="selectDashboardsText"
                :selections="selections"
                @on-select="onSelectDashboard"
            />
            <UiButton
                v-show="activeDashboardId"
                class="pf-m-primary"
                :before-icon="'fas fa-plus'"
                @click="newDashboard"
            >
                {{ $t('dashboards.newDashboard') }}
            </UiButton>
            <UiButton
                v-show="activeDashboardId"
                class="pf-u-ml-auto pf-m-link pf-m-danger"
                :before-icon="'fas fa-trash'"
                @click="onDeleteDashboard"
            >
                {{ $t('dashboards.delete') }}
            </UiButton>
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
        <div v-else>
            <div class="dashboards__name pf-u-mb-md">
                <UiInlineEdit
                    :value="dashboardName"
                    @on-input="(payload: string) => dashboardName = payload"
                />
            </div>
            <GridContainer>
                <GridItem
                    v-for="(item, i) in newDashboardRows"
                    :key="i"
                    :col-lg="12"
                    class="dashboards__panel"
                    :class="{
                        'dashboards__panel_padding': item?.panels.length >= 1 && item?.panels.length < 4
                    }"
                >
                    <div
                        v-if="item?.panels.length < 4"
                        class="dashboards__new"
                        :class="{
                            'dashboards__new_small': item?.panels.length >= 1
                        }"
                        @click="addReport(i)"
                    >
                        <div class="dashboards__new-item pf-u-box-shadow-sm pf-u-background-color-success pf-l-flex pf-m-align-items-center pf-m-justify-content-center">
                            <UiIcon
                                class="pf-u-font-size-3xl dashboards__new-item-icon"
                                :icon="'fas fa-plus-circle'"
                            />
                        </div>
                    </div>
                    <GridContainer>
                        <GridItem
                            v-for="panel in item?.panels"
                            :key="panel.reportId"
                            :col-lg="panel.span ?? 6"
                        >
                            <UiCard
                                v-if="panel?.report"
                                :title="panel?.report?.name"
                            >
                                <DashboardPanel
                                    :report="panel.report"
                                />
                            </UiCard>
                        </GridItem>
                    </GridContainer>
                </GridItem>
                <GridItem
                    v-if="newDashboardRows.length > 0 && newDashboardRows[newDashboardRows.length - 1].panels.length"
                    :col-lg="12"
                    class="dashboards__panel"
                >
                    <GridContainer>
                        <div
                            class="dashboards__new"
                            @click="addPanel"
                        >
                            <div class="dashboards__new-item pf-u-box-shadow-sm pf-u-background-color-success pf-l-flex pf-m-align-items-center pf-m-justify-content-center">
                                <UiIcon
                                    class="pf-u-font-size-3xl dashboards__new-item-icon"
                                    :icon="'fas fa-plus-circle'"
                                />
                            </div>
                        </div>
                    </GridContainer>
                </GridItem>
            </GridContainer>
        </div>
    </div>
    <DashboardReportsPopup
        v-if="dashboardReportsPopup"
        :reports="reports"
        :loading="loadingReports"
        @on-select-report="onSelectReport"
        @cancel="closeDashboardReportsPopup"
    />
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import dashboardService from '@/api/services/dashboards.service'
import reportsService from '@/api/services/reports.service'
import {
    Dashboard,
    ReportReportTypeEnum,
    Report,
    CreateDashboardRequest,
    CreateDashboardRequestRowsInner,
    CreateDashboardRequestRowsInnerPanelsInner,
} from '@/api'
import { pagesMap } from '@/router'
import useConfirm from '@/hooks/useConfirm'

import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import usei18n from '@/hooks/useI18n'

import UiSelect from '@/components/uikit/UiSelect.vue'
import GridContainer from '@/components/grid/GridContainer.vue'
import GridItem from '@/components/grid/GridItem.vue'
import UiCard from '@/components/uikit/UiCard/UiCard.vue'
import DashboardPanel from '@/components/dashboards/DashboardPanel.vue'
import DashboardReportsPopup from '@/components/dashboards/DashboardReportsPopup.vue'
import UiInlineEdit from '@/components/uikit/UiInlineEdit.vue'

const { confirm } = useConfirm()
const { t } = usei18n()
const commonStore = useCommonStore()
const lexiconStore = useLexiconStore()

interface DashboardPanelType {
    span: number,
    reportId?: number
    report?: Report,
}

interface DashboardRows {
    panels: DashboardPanelType[]
}

const loadingReports = ref(false)
const reports = ref<Report[]>([])
const dashboardName = ref(t('dashboards.untitledDashboard'))
const dashboardReportsPopup = ref(false)
const addReportPanels = ref(0)
const newDashboardRows = ref<DashboardRows[]>([])
const errorDashboard = ref(false)
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
    dashboardName.value = t('dashboards.untitledDashboard')
    newDashboardRows.value = []
}

const getReportsList = async () => {
    loadingReports.value = true
    try {
        const res = await reportsService.reportsList(commonStore.organizationId, commonStore.projectId)
        if (res.data?.dashboards) {
            reports.value = res.data.dashboards
        }
    } catch(error: unknown) {
        throw Error(JSON.stringify(error))
    }
    loadingReports.value = false
}

const newDashboard = () => {
    activeDashboardId.value = null
    newDashboardRows.value = [{
        panels: []
    }]
}

const onDeleteDashboard = async () => {
    try {
        if (activeDashboardId.value) {
            await confirm(t('dashboards.deleteConfirm', { name: `<b>${activeDashboard.value?.name}</b>` || '' }), {
                applyButton: t('common.apply'),
                cancelButton: t('common.cancel'),
                title: t('dashboards.delete'),
                applyButtonClass: 'pf-m-danger',
            })

            await dashboardService.deleteDashboard(commonStore.organizationId, commonStore.organizationId, activeDashboardId.value)
            getDashboardsList()
        }
    } catch(error) {
        errorDashboard.value = true
    }
}

const getDashboardsList = async () => {
    const res = await dashboardService.dashboardsList(commonStore.organizationId, commonStore.organizationId)

    if (res?.data?.dashboards) {
        dashboards.value = res.data.dashboards

        if (res.data.dashboards[0]?.id) {
            onSelectDashboard(res.data.dashboards[0].id)
        }
    }
}

const onSelectReport = (payload: number) => {
    newDashboardRows.value[addReportPanels.value].panels.push({
        span: 6,
        reportId: payload,
        report: reports.value.find(item => Number(item.id) === payload)
    })
    newDashboardRows.value = newDashboardRows.value.map(item => {
        return {
            panels: item.panels.map(panel => {
                return {
                    ...panel,
                    span: 12 / item.panels.length
                }
            })
        }
    })
    closeDashboardReportsPopup()
}

const closeDashboardReportsPopup = () => {
    dashboardReportsPopup.value = false
}

const addReport = (index: number) => {
    addReportPanels.value = index
    dashboardReportsPopup.value = true
    getReportsList()
}

const addPanel = () => {
    newDashboardRows.value.push({panels: []})
    addReport(newDashboardRows.value.length - 1)
}

onMounted(() => {
    lexiconStore.getEvents()
    lexiconStore.getEventProperties()
    lexiconStore.getUserProperties()
    getDashboardsList()
})
</script>

<style lang="scss">
.dashboards {
    &__new-item {
        min-height: 250px;
        cursor: pointer;
    }
    &__new-item-icon {
        color: var(--pf-global--success-color--100);
    }
    &__name {
        max-width: 300px;
        .pf-c-inline-edit__value {
            font-size: 20px;
        }
    }
    &__panel {
        position: relative;
        min-height: 250px !important;
        width: 100%;
        &_padding {
            padding-left: 120px;
        }
    }
    &__new {
        position: absolute;
        top: 0;
        left: 0;
        height: 100%;
        min-width: 250px;
        &_small {
            min-width: 108px;
        }
    }
    &__new-item {
        height: 100%;
    }
}
</style>