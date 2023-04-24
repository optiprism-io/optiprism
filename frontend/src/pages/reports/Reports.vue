<template>
    <section class="pf-c-page__main-section report">
        <UiTabs
            class="pf-u-mb-md"
            :items="items"
            @on-select="onSelectTab"
        />
        <div
            v-if="reportsStore.loading"
            class="pf-u-h-66vh pf-u-display-flex pf-u-align-items-center pf-u-justify-content-center"
        >
            <UiSpinner :size="'xl'" />
        </div>
        <template v-else>
            <div class="pf-u-mb-sm pf-u-display-flex pf-u-justify-content-space-between pf-u-align-items-center">
                <div
                    v-if="itemsReports.length && !editableNameReport"
                    class="pf-u-mr-md"
                >
                    <UiSelect
                        class="dashboards__select"
                        :items="itemsReports"
                        :text-button="reportSelectText"
                        :is-text-select="true"
                        :selections="[Number(reportsStore.reportId)]"
                        @on-select="onSelectReport"
                    />
                </div>
                <div class="report__name pf-u-mr-md">
                    <UiInlineEdit
                        :value="reportName"
                        :hide-text="!!reportName"
                        :placeholder-value="t('reports.untitledReport')"
                        @on-input="setNameReport"
                        @on-edit="onEditNameReport"
                    />
                </div>
                <UiButton
                    v-show="reportsStore.reportId"
                    class="pf-m-link report__nav-item report__nav-item_new"
                    :before-icon="'fas fa-plus'"
                    @click="router.push({ query: { id: null } })"
                >
                    {{ $t('reports.createReport') }}
                </UiButton>
                <UiButton
                    v-show="reportsStore.reportId"
                    class="pf-m-link pf-m-danger"
                    :before-icon="'fas fa-trash'"
                    @click="onDeleteReport"
                >
                    {{ $t('dashboards.delete') }}
                </UiButton>
                <UiSwitch
                    class="pf-u-ml-auto pf-u-mr-md"
                    :value="commonStore.syncReports"
                    :label="$t('reports.sync')"
                    @input="(value: boolean) => commonStore.syncReports = value"
                />
            </div>
            <router-view />
        </template>
    </section>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { pagesMap } from '@/router'
import usei18n from '@/hooks/useI18n'
import { ReportType } from '@/api'
import { reportToStores } from '@/utils/reportsMappings'

import useConfirm from '@/hooks/useConfirm'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { useReportsStore } from '@/stores/reports/reports'
import { useCommonStore } from '@/stores/common'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useSegmentsStore } from '@/stores/reports/segments'
import { useBreakdownsStore } from '@/stores/reports/breakdowns'
import { useStepsStore } from '@/stores/funnels/steps'

import UiSelect from '@/components/uikit/UiSelect.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import UiInlineEdit from '@/components/uikit/UiInlineEdit.vue'
import UiSpinner from '@/components/uikit/UiSpinner.vue'

const { t } = usei18n()
const route = useRoute()
const router = useRouter()
const eventsStore = useEventsStore()
const reportsStore = useReportsStore()
const commonStore = useCommonStore()
const filterGroupsStore = useFilterGroupsStore()
const segmentsStore = useSegmentsStore()
const breakdownsStore = useBreakdownsStore()
const stepsStore = useStepsStore()
const { confirm } = useConfirm()

const editableNameReport = ref(false);
const reportName = ref('');

const items = computed(() => {
    const mapTabs = [
        {
            name: t('events.event_segmentation'),
            value: pagesMap.reportsEventSegmentation.name,
            link: {
                name: pagesMap.reportsEventSegmentation.name,
            },
        },
        {
            name: t('funnels.funnels'),
            value: 'reports_funnels',
            link: {
                name: 'reports_funnels'
            },
        }
    ];

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value,
        }
    });
})

const reportSelectText = computed(() => {
    return reportsStore.activeReport ? reportsStore.activeReport.name : t('reports.selectReport')
})

const isChangedReport = computed(() => {
    return reportsStore.saveLoading || reportsStore.isChangedReport || reportsStore?.activeReport?.name !== reportName.value
})

const reportType = computed(() => pagesMap.reportsEventSegmentation.name === route.name ?
    ReportType.EventSegmentation : ReportType.Funnel)

const itemsReports = computed(() => {
    return reportsStore.list.map(item => {
        const id = Number(item.id)
        return {
            value: id,
            key: id,
            nameDisplay: item.name || '',
        }
    })
})

const onEditNameReport = (payload: boolean) => {
    editableNameReport.value = payload;
};

const onDeleteReport = async () => {
    try {
        await confirm(t('reports.deleteConfirm', { name: `<b>${reportsStore?.activeReport?.name}</b>` || '' }), {
            applyButton: t('common.apply'),
            cancelButton: t('common.cancel'),
            title: t('reports.delete'),
            applyButtonClass: 'pf-m-danger',
        })

        reportsStore.deleteReport(reportsStore.reportId)
        setEmptyReport()
        reportsStore.getList()
    } catch(error) {
        reportsStore.loading = false
    }
}

const onSaveReport = async () => {
    if (reportsStore.reportId) {
        await reportsStore.editReport(reportName.value, reportType.value)
    } else {
        await reportsStore.createReport(reportName.value || t('reports.untitledReport'), reportType.value)

        router.push({
            params: {
                id: reportsStore.reportId,
            }
        })
    }
    await reportsStore.getList()
    reportsStore.updateDump(reportType.value)
}

const setNameReport = (payload: string) => {
    reportName.value = payload
    onSaveReport()
}

const setEmptyReport = () => {
    reportsStore.reportId = 0
    reportName.value = '';
    eventsStore.$reset()
    filterGroupsStore.$reset()
    segmentsStore.$reset()
    breakdownsStore.$reset()
    stepsStore.$reset()
}

const onSelectTab = () => {
    if (reportsStore.reportId) {
        setEmptyReport()
    }
}

const updateReport = async (id: number) => {
    try {
        await reportToStores(Number(id))
    } catch(e) {
        throw new Error('cannot update report');
    }
    reportName.value = reportsStore.activeReport?.name ?? t('reports.untitledReport')
    reportsStore.updateDump(reportType.value)
}

const onSelectReport = (id: number) => {
    updateReport(id)
    router.push({ params: { id } })
}

onMounted(async () => {
    reportsStore.loading = true
    eventsStore.initPeriod()
    await reportsStore.getList()
    const reportId = route.params.id;
    if (reportId && reportsStore?.reportsId.includes(Number(reportId))) {
        updateReport(Number(reportId))
    } else {
        reportsStore.reportId = 0
        router.push({
            params: {
                id: null,
            }
        })
    }
    reportsStore.loading = false
})
</script>

<style lang="scss">
.report {
    &__name {
        .pf-c-inline-edit__value {
            font-size: 20px;
        }
    }
    &__select {
        width: 200px;
    }
    &__nav {
        min-height: 34px;
        &-item {
            &_new {
                margin-left: -12px;
            }
        }
    }
}
</style>