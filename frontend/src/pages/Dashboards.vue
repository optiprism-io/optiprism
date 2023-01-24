<template>
    <div class="dashboards pf-u-p-md pf-u-pb-3xl">
        <div>
            <div class="pf-u-mb-md pf-u-display-flex">
                <div class="dashboards__name pf-u-mr-md">
                    <UiInlineEdit
                        :value="dashboardName"
                        @on-input="updateName"
                    />
                </div>
                <UiSelect
                    v-if="reportsList.length"
                    class=" pf-u-mr-md dashboards__add-report"
                    :items="selectReportsList"
                    :text-button="t('dashboards.addReport')"
                    @on-select="addReport"
                >
                    <template #action>
                        <UiButton
                            class="dashboards__add-report-button pf-m-main"
                        >
                            {{ t('dashboards.addReport') }}
                        </UiButton>
                    </template>
                </UiSelect>
            </div>
            <GridLayout
                v-model:layout="layout"
                :col-num="12"
                :row-height="ROW_HEIGHT"
                :minW="3"
                :minH="4"
                :useCssTransforms="false"
            >
                <template #default="{ gridItemProps }">
                    <GridItem
                        v-for="item in layout"
                        :key="item.i"
                        v-bind="gridItemProps"
                        :x="item.x"
                        :y="item.y"
                        :w="item.w"
                        :h="item.h"
                        :i="item.i"
                        :minH="item.minH"
                        :minW="item.minW"
                        @moved="moved"
                        @resized="resized"
                    >
                        <UiCard>
                            <template #rightTitle>
                                <UiDropdown
                                    class="pf-u-mr-md"
                                    :items="menuCardReport"
                                    :has-icon-arrow-button="false"
                                    :transparent="true"
                                    :placement-menu="'bottom-end'"
                                    @select-value="(paylaod: UiDropdownItem<string>) => selectReportDropdown(paylaod, item.i)"
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
                                :height-chart="item.h * ROW_HEIGHT - 20"
                                :report-id="item.reportId"
                            />
                        </UiCard>
                    </GridItem>
                </template>
            </GridLayout>
        </div>
    </div>
    <DashboardReportsPopup
        v-if="dashboardReportsPopup"
        :reports="reportsList"
        :loading="false"
        @on-select-report="onSelectReport"
        @cancel="closeDashboardReportsPopup"
    />
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { pagesMap } from '@/router'
import dashboardService from '@/api/services/dashboards.service'
import reportsService from '@/api/services/reports.service'
import {
    Report,
} from '@/api'
import { DashboardPanel as DashboardPanelType, DashboardPanelTypeEnum } from '@/api'
import useConfirm from '@/hooks/useConfirm'
import { useDashboardsStore } from '@/stores/dashboards'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import { useReportsStore } from '@/stores/reports/reports'
import usei18n from '@/hooks/useI18n'

import UiCard from '@/components/uikit/UiCard/UiCard.vue'
import DashboardPanel from '@/components/dashboards/DashboardPanel.vue'
import DashboardReportsPopup from '@/components/dashboards/DashboardReportsPopup.vue'
import UiInlineEdit from '@/components/uikit/UiInlineEdit.vue'
import { GenericUiDropdown, UiDropdownItem } from '@/components/uikit/UiDropdown.vue'
import UiSelect from '@/components/uikit/UiSelect.vue';
import UiButton from '@/components/uikit/UiButton.vue';

const UiDropwdown = GenericUiDropdown<string>()
const { confirm } = useConfirm()
const { t } = usei18n()
const route = useRoute()
const router = useRouter()
const commonStore = useCommonStore()
const lexiconStore = useLexiconStore()
const dashboardsStore = useDashboardsStore()
const reportsStore = useReportsStore()

const ROW_HEIGHT = 56;

interface DashboardRows {
    panels: DashboardPanelType[]
}

interface Layout extends DashboardPanelType {
    i: number
    minH: number,
    minW: number,
}

const layout = ref<Layout[]>([]);
const loadingReports = ref(false)
const reports = ref<Report[]>([])
const dashboardReportsPopup = ref(false)
const addReportPanels = ref(0)
const dashboardName = ref(t('dashboards.untitledDashboard'))
const newDashboardRows = ref<DashboardRows[]>([])
const errorDashboard = ref(false)
const activeDashboardId = ref<number | null>(null)
const editPanel = ref<number | null>(null)
const dashboards = computed(() => dashboardsStore.dashboards)
const reportsList = computed(() => reportsStore.list)

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
            key: 0,
            value: 'edit',
            nameDisplay: t('dashboards.changeReport'),
        },
        {
            key: 1,
            value: 'delete',
            nameDisplay: t('common.delete'),
        }
    ]
})

const selectReportsList = computed(() => {
    return reportsList.value.map(item => {
        const id = Number(item.id)
        return {
            value: id,
            key: id,
            nameDisplay: item.name || '',
        }
    })
})

const moved = () => {
    updateCreateDashboard();
}

const resized = () => {
    updateCreateDashboard();
}

const updateLauout = () => {
    if (activeDashboard.value) {
        dashboardName.value = activeDashboard.value?.name || t('dashboards.untitledDashboard')
        const newLayout = activeDashboard.value?.panels?.map((item: DashboardPanelType, i: number) => {
            return {
                ...item,
                i,
                minH: 5,
                minW: 3,
            }
        }) || [];

        if (JSON.stringify(newLayout) !== JSON.stringify(layout.value)) {
            layout.value = newLayout;
        }
    }
}

const onSelectDashboard = (id: number | string) => {
    activeDashboardId.value = Number(id);
    router.push({ query: { id } })
    updateLauout();
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
    await dashboardsStore.getDashboards()
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

const updateCreateDashboard = async (panels?: Layout[]) => {
    try {
        const dataForRequest = {
            name: dashboardName.value,
            panels: (panels || layout.value).map(item => {
                return {
                    type: DashboardPanelTypeEnum.Report,
                    reportId: item.reportId,
                    i: item.i,
                    x: item.x,
                    y: item.y,
                    w: item.w,
                    h: item.h,
                }
            })
        }
        if (activeDashboardId.value) {
            await dashboardService.updateDashboard(commonStore.organizationId, commonStore.projectId, activeDashboardId.value, dataForRequest)
        } else {
            const res = await dashboardService.createDashboard(commonStore.organizationId, commonStore.projectId, dataForRequest)
            if (res.data?.id) {
                router.push({
                    name: pagesMap.dashboards.name,
                    query: {
                        id: res.data.id,
                    },
                })
            }
        }
    } catch (e) {
        console.error(e);
    }
}

const onSelectReport = (payload: number) => {
    const items = layout.value;
    const panelIndex = items.findIndex(item => Number(item.i) === editPanel.value)
    items[panelIndex].reportId = payload;
    layout.value = items;
    closeDashboardReportsPopup()
    updateCreateDashboard()
}

const closeDashboardReportsPopup = () => {
    dashboardReportsPopup.value = false
    editPanel.value = null
}

const addReport = async (payload: number) => {
    const panel = {
        type: DashboardPanelTypeEnum.Report,
        reportId: payload,
        i: layout.value.length + 1,
        x: 0,
        y: 0,
        w: 3,
        h: 5,
        minH: 5,
        minW: 3,
    }
    await updateCreateDashboard([panel, ...layout.value]);
    layout.value = [panel, ...layout.value];
}

const selectReportDropdown = async (payload: UiDropdownItem<string>, id: number) => {
    if (payload.value === 'delete') {
        layout.value = layout.value.filter(item => item.i !== id);
        setTimeout(() => {
            updateCreateDashboard();
        }, 800)
    }
    if (payload.value === 'edit') {
        editPanel.value = id;
        dashboardReportsPopup.value = true;
    }
}

const setNew = () => {
    layout.value = [];
    activeDashboardId.value = null
    dashboardName.value = t('dashboards.untitledDashboard')
    router.push({
        query: {
            id: null,
        }
    })
}

const updateName = async (payload: string) => {
    dashboardName.value = payload
    await updateCreateDashboard()
    getDashboardsList()
}

const initDashboardPage = () => {
    const dashboardId = route.query.id;

    if (dashboardId) {
        if (dashboardsId.value.includes(Number(dashboardId))) {
            onSelectDashboard(Number(dashboardId))
        } else {
            setNew()
        }
    } else {
        if (!route.query.new && dashboards.value.length && dashboards.value[0].id) {
            onSelectDashboard(Number(dashboards.value[0].id))
        } else {
            setNew()
        }
    }
};

onMounted(async () => {
    lexiconStore.getEvents()
    lexiconStore.getEventProperties()
    lexiconStore.getUserProperties()
    if (!dashboards.value?.length) {
        await getDashboardsList()
    }
    initDashboardPage()
})

watch(() => route.query.id, id => {
    if (Number(id) !== activeDashboardId.value) {
        if (id) {
            onSelectDashboard(Number(id))
        } else {
            setNew();
        }
    }
})
</script>

<style lang="scss">
.dashboards {
    .vue-grid-item {
        .pf-c-card__body {
            height: calc(100% - 36px);
        }
    }
    &__add-report-button {
        width: 100%;
    }
    &__add-report {
        max-width: 140px;
    }
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