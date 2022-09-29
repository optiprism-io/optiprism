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
                <div class="report__name">
                    <UiInlineEdit
                        :value="reportName"
                        @on-input="setNameReport"
                    />
                </div>
                <UiSwitch
                    class="pf-u-ml-auto"
                    :value="commonStore.syncReports"
                    :label="$t('reports.sync')"
                    @input="(value: boolean) => commonStore.syncReports = value"
                />
                <div class="pf-u-ml-lg">
                    <UiSelect
                        :items="itemsReports"
                        :text-button="reportSelectText"
                        :selections="[reportsStore.reportId]"
                        @on-select="onSelectReport"
                    />
                </div>
                <UiButton
                    class="pf-m-primary pf-u-ml-lg"
                    :progress="reportsStore.saveLoading"
                    @click="onSaveReport"
                >
                    {{ $t('reports.save') }}
                </UiButton>
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
import { useLexiconStore } from '@/stores/lexicon'
import { useEventsStore } from '@/stores/eventSegmentation/events'
import { ReportReportTypeEnum } from '@/api'
import { reportToEvents, reportToFunnels } from '@/utils/reportsMappings'

import { useReportsStore } from '@/stores/reports/reports'
import { useCommonStore } from '@/stores/common'
import { useFilterGroupsStore } from '@/stores/reports/filters'
import { useSegmentsStore } from '@/stores/reports/segments'

import UiSelect from '@/components/uikit/UiSelect.vue'
import UiSwitch from '@/components/uikit/UiSwitch.vue'
import UiInlineEdit from '@/components/uikit/UiInlineEdit.vue'
import UiSpinner from '@/components/uikit/UiSpinner.vue'

const { t } = usei18n()
const route = useRoute()
const router = useRouter()
const lexiconStore = useLexiconStore()
const eventsStore = useEventsStore()
const reportsStore = useReportsStore()
const commonStore = useCommonStore()
const filterGroupsStore = useFilterGroupsStore()
const segmentsStore = useSegmentsStore()

const reportName = ref(t('reports.untitledReport'))

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
    })
})

const reportSelectText = computed(() => {
    return reportsStore.activeReport ? reportsStore.activeReport.name : t('reports.selectReport')
})

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

const setNameReport = (payload: string) => {
    reportName.value = payload
}

const onSaveReport = async () => {
    if (reportsStore.reportId) {
        // TODO ReportReportTypeEnum
        await reportsStore.editReport(reportName.value, ReportReportTypeEnum.EventSegmentation)
    } else {
        await reportsStore.createReport(reportName.value, ReportReportTypeEnum.EventSegmentation)

        router.push({
            params: {
                id: reportsStore.reportId,
            }
        })
    }
    await reportsStore.getList()
}

const onSelectTab = () => {
    if (reportsStore.reportId) {
        reportsStore.reportId = 0
        reportName.value = t('reports.untitledReport')
        eventsStore.$reset()
        filterGroupsStore.$reset()
        segmentsStore.$reset()
    }
}

const updateReport = async (id: number) => {
    if (pagesMap.reportsEventSegmentation.name === route.name) {
        reportToEvents(Number(id))
    } else {
        reportToFunnels(Number(id))
    }

    reportName.value = reportsStore.activeReport?.name ?? t('reports.untitledReport')
}

const onSelectReport = (id: number) => {
    updateReport(id)
    router.push({ params: { id } })
}

onMounted(async () => {
    reportsStore.loading = true
    lexiconStore.getEvents()
    lexiconStore.getEventProperties()
    lexiconStore.getUserProperties()
    eventsStore.initPeriod()
    await reportsStore.getList()
    const reportId = route.params.id;

    if (reportId) {
        if (reportsStore.reportsId.includes(Number(reportId))) {
            updateReport(Number(reportId))
        } else {
            reportsStore.reportId = 0
            router.push({
                params: {
                    id: null,
                }
            })
        }
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
}

</style>