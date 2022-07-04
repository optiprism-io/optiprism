<template>
    <div class="pf-c-toolbar">
        <div class="pf-c-toolbar__content">
            <div class="pf-c-toolbar__content-section pf-m-nowrap">
                <div class="pf-c-toolbar__item">
                    <UiToggleGroup
                        :items="itemsPeriod"
                        @select="onSelectPerion"
                    >
                        <template #after>
                            <UiDatePicker
                                :value="calendarValue"
                                :last-count="lastCount"
                                :active-tab-controls="liveStreamStore.period.type"
                                @on-apply="onApplyPeriod"
                            >
                                <template #action>
                                    <button
                                        class="pf-c-toggle-group__button"
                                        :class="{
                                            'pf-m-selected': liveStreamStore.isPeriodActive,
                                        }"
                                        type="button"
                                    >
                                        <div class="pf-u-display-flex pf-u-align-items-center">
                                            <UiIcon :icon="'far fa-calendar-alt'" />
                                            &nbsp;
                                            {{ calendarValueString }}
                                        </div>
                                    </button>
                                </template>
                            </UiDatePicker>
                        </template>
                    </UiToggleGroup>
                </div>
                <div
                    v-if="liveStreamStore.columnsMap.length"
                    class="pf-c-toolbar__item pf-u-ml-auto"
                >
                    <UiSelect
                        :items="columns"
                        :variant="'multiple'"
                        :text-button="columnsButtonText"
                        :selections="liveStreamStore.activeColumns"
                        @on-select="liveStreamStore.toggleColumns"
                    />
                </div>
            </div>
        </div>
    </div>
    <div class="pf-c-scroll-inner-wrapper">
        <div
            class="pf-u-min-height"
            style="--pf-u-min-height--MinHeight: 24ch;"
        >
            <div
                v-if="liveStreamStore.isNoData"
                class="pf-u-display-flex pf-u-justify-content-center pf-u-align-items-center pf-u-h-100"
            >
                <div>
                    <div class="pf-u-m-auto pf-u-w-25 pf-u-color-400 pf-u-text-align-center">
                        <UiIcon
                            class="pf-u-font-size-4xl"
                            :icon="'fas fa-search'"
                        />
                    </div>
                    <div class="pf-c-card__title pf-u-text-align-center pf-u-font-size-lg pf-u-color-400">
                        {{ $t('events.select_to_start') }}
                    </div>
                </div>
            </div>
            <div
                v-else-if="liveStreamStore.loading"
                class="pf-u-display-flex pf-u-justify-content-center pf-u-align-items-center pf-u-h-100"
            >
                <UiSpinner :size="'xl'" />
            </div>
            <UiTable
                v-else
                :items="tableData"
                :columns="tableColumnsValues"
                @on-action="onAction"
            />
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue'
import { getStringDateByFormat } from '@/helpers/getStringDates'

import { useLiveStreamStore, Report } from '@/stores/reports/liveStream'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import { useEventsStore } from '@/stores/eventSegmentation/events'

import { ApplyPayload } from '@/components/uikit/UiCalendar/UiCalendar'
import { Column, Cell, Action,  } from '@/components/uikit/UiTable/UiTable'
import EventCell, { EventCell as EventCellType } from '@/components/events/EventCell.vue'

import UiToggleGroup, { UiToggleGroupItem } from '@/components/uikit/UiToggleGroup.vue'
import UiDatePicker from '@/components/uikit/UiDatePicker.vue'
import UiSelect from '@/components/uikit/UiSelect.vue'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import UiSpinner from '@/components/uikit/UiSpinner.vue'

const i18n = inject<any>('i18n')
const liveStreamStore = useLiveStreamStore()
const commonStore = useCommonStore()
const lexiconStore = useLexiconStore()
const eventsStore = useEventsStore()

const itemsPeriod = computed(() => {
    return ['7', '30', '90'].map((key, i): UiToggleGroupItem => ({
        key,
        nameDisplay: key + i18n.$t('common.calendar.day_short'),
        value: key,
        selected: liveStreamStore.controlsPeriod === key,
    }))
})

const updateReport = () => {
    liveStreamStore.getReportLiveStream()
}

const tableColumnsValues = computed(() => {
    return [
        ...liveStreamStore.defaultColumns.map((key, i) => {
            return {
                fixed: true,
                value: key,
                title: i18n.$t(`events.live_stream.columns.${key}`),
                truncate: true,
                lastFixed: liveStreamStore.defaultColumns.length - 1 === i,
            }
        }),
        ...liveStreamStore.activeColumns.map(key => {
            return {
                value: key,
                title: key.charAt(0).toUpperCase() + key.slice(1),
            }
        })
    ]
})

const tableData = computed(() => {
    return liveStreamStore.reports.map((data: Report) => {
        return tableColumnsValues.value.map((column: Column): Cell | EventCellType => {
            if (liveStreamStore.defaultColumns.includes(column.value)) {
                const value = column.value === 'eventName' ? data.name : getStringDateByFormat(String(data.properties[column.value]), '%d %b, %Y')

                return {
                    value: value,
                    title: value,
                    fixed: true,
                    lastFixed: column.lastFixed,
                    actions: column.value === 'eventName' ? [{
                        name: 'create',
                        icon: 'fas fa-plus-circle'
                    }] : [],
                    customEvents: column.value === 'eventName' && lexiconStore.customEvents?.length && Array.isArray(data.matchedCustomEvents) ? data.matchedCustomEvents.map(event => {
                        const customEvent = lexiconStore.findCustomEventById(Number(event.id))

                        return {
                            name: customEvent.name,
                            value: Number(event.id)
                        }
                    }) : [],
                    component: column.value === 'eventName' ? EventCell : null,
                }
            } else {
                const value = column.value in data.properties ? data.properties[column.value] : data.userProperties && column.value in data.userProperties ? data.userProperties[column.value] : ''

                return {
                    value: value,
                    title: value || '-'
                }
            }
        })
    })
})

const perios = computed(() => {
    return liveStreamStore.period
})

const lastCount = computed(() => {
    return perios.value.last
})

const calendarValue = computed(() => {
    return {
        from: perios.value.from,
        to: perios.value.to,
        multiple: false,
        dates: [],
    }
})

const columnsButtonText = computed(() => {
    return `${liveStreamStore.columnsMap.length} ${i18n.$t('common.columns')}`
})

const columns = computed(() => {
    return liveStreamStore.columnsMap.map(key => {
        return {
            key: key,
            nameDisplay: key.charAt(0).toUpperCase() + key.slice(1),
            value: key,
        }
    })
})

const calendarValueString = computed(() => {
    if (liveStreamStore.isPeriodActive) {
        switch(liveStreamStore.period.type) {
            case 'last':
                return `${i18n.$t('common.calendar.last')} ${liveStreamStore.period.last} ${i18n.$t(liveStreamStore.period.last === 1 ? 'common.calendar.day' : 'common.calendar.days')}`
            case 'since':
                return `${i18n.$t('common.calendar.since')} ${getStringDateByFormat(liveStreamStore.period.from, '%d %b, %Y')}`
            case 'between':
                return `${getStringDateByFormat(liveStreamStore.period.from, '%d %b, %Y')} - ${getStringDateByFormat(liveStreamStore.period.to, '%d %b, %Y')}`
            default:
                return i18n.$t('common.castom')
        }
    } else {
        return i18n.$t('common.castom')
    }
})


const onSelectPerion = (payload: string) => {
    liveStreamStore.controlsPeriod = payload
    liveStreamStore.period.type = 'notCustom'
    updateReport()
}

const onApplyPeriod = (payload: ApplyPayload): void => {
    liveStreamStore.controlsPeriod = 'calendar'
    liveStreamStore.period = {
        ...liveStreamStore.period,
        from: payload.value.from || '',
        to: payload.value.to || '',
        type: payload.type,
        last: payload.last,
    }

    updateReport()
}

const onAction = (payload: Action) => {
    if (payload.name === 'create') {
        eventsStore.setEditCustomEvent(null)
        commonStore.togglePopupCreateCustomEvent(true)
    }

    if (payload.type === 'event') {
        eventsStore.setEditCustomEvent(Number(payload.name))
        commonStore.togglePopupCreateCustomEvent(true)
    }
}
</script>

<style scoped lang="scss">
</style>