<template>
    <div class="dashboards pf-u-p-md pf-u-pb-3xl">
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
                @click="setNew"
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
        <div>
            <div class="dashboards__name pf-u-mb-md">
                <UiInlineEdit
                    :value="dashboardName"
                    @on-input="updateName"
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
                        <div class="dashboards__new-item pf-u-box-shadow-sm pf-l-flex pf-m-align-items-center pf-m-justify-content-center">
                            <UiIcon
                                class="pf-u-font-size-xl dashboards__new-item-icon"
                                :icon="'fas fa-plus'"
                            />
                        </div>
                    </div>
                    <GridContainer>
                        <GridItem
                            v-for="(panel, j) in item?.panels"
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
                                <template #rightTitle>
                                    <UiDropdown
                                        class="pf-u-mr-md"
                                        :items="menuCardReport"
                                        :has-icon-arrow-button="false"
                                        :transparent="true"
                                        :placement-menu="'bottom-end'"
                                        @select-value="(paylaod) => selectReportDropdown(paylaod, i, j)"
                                    >
                                        <template #button>
                                            <button
                                                class="pf-c-dropdown__toggle pf-m-plain"
                                                aria-expanded="true"
                                                type="button"
                                                aria-label="Actions"
                                            >
                                                <i
                                                    class="fas fa-ellipsis-v"
                                                    aria-hidden="true"
                                                />
                                            </button>
                                        </template>
                                    </UiDropdown>
                                </template>
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
                            <div class="dashboards__new-item dashboards__new-item_small pf-u-box-shadow-sm pf-l-flex pf-m-align-items-center pf-m-justify-content-center">
                                <UiIcon
                                    class="pf-u-font-size-xl dashboards__new-item-icon"
                                    :icon="'fas fa-plus'"
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
import { useRoute, useRouter } from 'vue-router'
import dashboardService from '@/api/services/dashboards.service'
import reportsService from '@/api/services/reports.service'
import {
    Dashboard,
    ReportReportTypeEnum,
    Report,
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
import { GenericUiDropdown, UiDropdownItem } from '@/components/uikit/UiDropdown.vue'

const { confirm } = useConfirm()
const { t } = usei18n()
const route = useRoute()
const router = useRouter()
const commonStore = useCommonStore()
const lexiconStore = useLexiconStore()
const UiDropdown = GenericUiDropdown<string>()

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
const dashboardReportsPopup = ref(false)
const addReportPanels = ref(0)
const dashboardName = ref(t('dashboards.untitledDashboard'))
const newDashboardRows = ref<DashboardRows[]>([])
const errorDashboard = ref(false)
const dashboards = ref<Dashboard[]>([])
const activeDashboardId = ref<number | null>(null)
const dashboardsId = computed((): number[] => {
    return dashboards.value.map(item => Number(item.id))
});

const selections = computed(() => activeDashboardId.value ? [activeDashboardId.value] : [])
const activeDashboard = computed(() => {
    return dashboards.value.find(item => Number(item.id) === activeDashboardId.value) ?? null
})

const menuCardReport = computed<UiDropdownItem<string>[]>(() => {
    return [
        {
            key: 1,
            value: 'delete',
            nameDisplay: t('common.delete'),
        }
    ]
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
    router.push({ query: { id } })
    if (activeDashboard.value) {
        dashboardName.value = activeDashboard.value?.name || t('dashboards.untitledDashboard')
        newDashboardRows.value = activeDashboard.value.rows?.map((item): DashboardRows => {
            return {
                panels: item.panels?.map((panel): DashboardPanelType => {
                    return {
                        span: panel.span || 6,
                        reportId: panel.reportId,
                        report: panel.report
                    }
                }) || []
            }
        }) || []
    }
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
        }
    } catch(error) {
        errorDashboard.value = true
    }
}

const getDashboardsList = async () => {
    const res = await dashboardService.dashboardsList(commonStore.organizationId, commonStore.organizationId)

    if (res?.data?.dashboards) {
        dashboards.value = res.data.dashboards
    }
}

const updateDashboardSpan = (dashboard: DashboardRows[]) => {
    return dashboard.map(item => {
        return {
            panels: item.panels.map(panel => {
                return {
                    ...panel,
                    span: 12 / item.panels.length
                }
            })
        }
    })
}

const updateCreateDashboard = async () => {
    const dataForRequest = {
        name: dashboardName.value,
        rows: newDashboardRows.value.map(item => {
            return {
                panels: item.panels.map((item) => {
                    return {
                        span: item.span,
                        reportId: Number(item.reportId),
                        report: item.report,
                    };
                })
            };
        }),
    }
    if (activeDashboardId.value) {
        await dashboardService.updateDashboard(commonStore.organizationId, commonStore.projectId, String(activeDashboardId.value), dataForRequest)
    } else {
        await dashboardService.createDashboard(commonStore.organizationId, commonStore.projectId, dataForRequest)
    }
    getDashboardsList()
}

const onSelectReport = (payload: number) => {
    newDashboardRows.value[addReportPanels.value].panels.push({
        span: 6,
        reportId: payload,
        report: reports.value.find(item => Number(item.id) === payload)
    })
    newDashboardRows.value = updateDashboardSpan(newDashboardRows.value)
    closeDashboardReportsPopup()
    updateCreateDashboard()
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

const selectReportDropdown = (payload: UiDropdownItem<string>, indexRow: number, indexPanel: number) => {
    if (payload.value === 'delete') {
        const dashboard = [...newDashboardRows.value]
        const row = dashboard[indexRow]
        if (row.panels.length === 1 && indexRow) {
            dashboard.splice(indexRow, 1);
        } else {
            row.panels.splice(indexPanel, 1)
            dashboard[indexRow] = row
        }
        newDashboardRows.value = updateDashboardSpan(dashboard)
    }
    updateCreateDashboard()
}

const setNew = () => {
    newDashboardRows.value = [{
        panels: []
    }]
    activeDashboardId.value = null
    dashboardName.value = t('dashboards.untitledDashboard')
    router.push({
        query: {
            id: null,
        }
    })
}

const updateName = (payload: string) => {
    dashboardName.value = payload
    updateCreateDashboard()
}

onMounted(async () => {
    lexiconStore.getEvents()
    lexiconStore.getEventProperties()
    lexiconStore.getUserProperties()
    await getDashboardsList()
    const dashboardId = route.query.id;

    if (dashboardId) {
        if (dashboardsId.value.includes(Number(dashboardId))) {
            onSelectDashboard(Number(dashboardId))
        } else {
            setNew()
        }
    } else {
        if (dashboards.value.length && dashboards.value[0].id) {
            onSelectDashboard(Number(dashboards.value[0].id))
        } else {
            setNew()
        }
    }
})
</script>

<style lang="scss">
.dashboards {
    &__new-item {
        min-height: 250px;
        height: 100%;
        cursor: pointer;
        background-color: var(--pf-global--palette--green-50);
        &_small {
            min-height: 50px;
        }
    }
    &__new-item-icon {
        color: var(--pf-global--Color--300);
    }
    &__name {
        max-width: 300px;
        .pf-c-inline-edit__value {
            font-size: 20px;
        }
    }
    &__panel {
        position: relative;
        min-height: 250px;
        width: 100%;
        &_padding {
            padding-left: 65px;
        }
    }
    &__new {
        position: absolute;
        top: 0;
        left: 0;
        height: 100%;
        width: 100%;
        &_small {
            max-width: 50px;
        }
    }
}
</style>