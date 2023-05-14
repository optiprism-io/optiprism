<template>
    <div
        class="pf-u-min-height"
        style="--pf-u-min-height--MinHeight: 24ch;"
    >
        <DataEmptyPlaceholder v-if="liveStreamStore.isNoData">
            {{ $t('events.select_to_start') }}
        </DataEmptyPlaceholder>
        <UiTable
            :is-loading="liveStreamStore.loading"
            :items="tableData"
            :columns="tableColumnsValues"
            @on-action="onAction"
        >
            <template #before>
                <UiToggleGroup
                    :items="itemsPeriod"
                    @select="onSelectPerion"
                >
                    <template #after>
                        <UiDatePicker
                            :value="calendarValue"
                            :last-count="liveStreamStore.period.last"
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
            </template>
            <template #after>
                <div
                    v-if="liveStreamStore.columnsMap.length"
                    class="pf-c-toolbar__item pf-u-ml-auto"
                >
                    <Select
                        :items="selectColumns"
                        :text-button="columnsButtonText"
                        :multiple="true"
                        :show-search="false"
                        :grouped="true"
                        class="pf-c-select"
                        @select="liveStreamStore.toggleColumns"
                    >
                        <UiButton
                            class="pf-c-select__toggle"
                            type="button"
                        >
                            {{ columnsButtonText }}
                            <span
                                class="pf-c-select__toggle-arrow"
                            >
                                <i
                                    class="fas fa-caret-down"
                                    aria-hidden="true"
                                />
                            </span>
                        </UiButton>
                    </Select>
                </div>
            </template>
        </UiTable>
        <LiveStreamEventPopup
            v-if="liveStreamStore.eventPopup"
            :name="eventPopupName"
        />
    </div>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { getStringDateByFormat } from '@/helpers/getStringDates'
import { componentsMaps } from '@/configs/events/liveStreamTableDefault'
import { useLiveStreamStore, Report } from '@/stores/reports/liveStream'
import { useCommonStore } from '@/stores/common'
import { useLexiconStore } from '@/stores/lexicon'
import { useEventsStore } from '@/stores/eventSegmentation/events'

import { shortPeriodDays } from '@/components/uikit/UiCalendar/UiCalendar.config';
import { ApplyPayload } from '@/components/uikit/UiCalendar/UiCalendar'
import { Column, Cell, Action,  } from '@/components/uikit/UiTable/UiTable'
import { EventCell as EventCellType } from '@/components/events/EventCell.vue'
import UiToggleGroup, { UiToggleGroupItem } from '@/components/uikit/UiToggleGroup.vue'
import UiDatePicker from '@/components/uikit/UiDatePicker.vue'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import DataEmptyPlaceholder from '@/components/common/data/DataEmptyPlaceholder.vue';
import UiCellToolMenu from '@/components/uikit/cells/UiCellToolMenu.vue'
import LiveStreamEventPopup from '@/components/events/LiveStreamEventPopup.vue'
import Select from '@/components/Select/Select.vue'
import { Group, Item } from '@/components/Select/SelectTypes'

const i18n = inject<any>('i18n')
const liveStreamStore = useLiveStreamStore()
const commonStore = useCommonStore()
const lexiconStore = useLexiconStore()
const eventsStore = useEventsStore()

const actionTable = 'action'
const createCustomEvent = 'create'
const customEvents = 'customEvents'
const eventName = 'eventName'
const properties = 'properties'
const userProperties = 'userProperties'

const eventPopupName = ref('')

const itemsPeriod = computed(() => {
    return shortPeriodDays.map((key): UiToggleGroupItem => ({
        key,
        nameDisplay: key + i18n.$t('common.calendar.dayShort'),
        value: key,
        selected: liveStreamStore.controlsPeriod === key,
    }))
})

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
        ...liveStreamStore.columnsMap.filter(key => liveStreamStore.activeColumns.includes(key)).map(key => {
            return {
                value: key,
                title: key.charAt(0).toUpperCase() + key.slice(1),
                fitContent: true,
                noWrap: true,
            }
        }),
        {
            value: actionTable,
            title: '',
            default: true,
            type: actionTable,
        }
    ]
})

const tableData = computed(() => {
    return liveStreamStore.reports.map((data: Report) => {
        return tableColumnsValues.value.map((column: Column): Cell | EventCellType => {
            if (liveStreamStore.defaultColumns.includes(column.value)) {
                const value = column.value === 'eventName' ? data.name : getStringDateByFormat(String(data.properties[column.value]), '%d %b, %Y')

                const cell: Cell | EventCellType = {
                    key: column.value,
                    title: value,
                    fixed: true,
                    lastFixed: column.lastFixed,
                    actions: column.value === customEvents ? [{
                        name: 'create',
                        icon: 'fas fa-plus-circle'
                    }] : [],
                    customEvents: column.value === customEvents && lexiconStore.customEvents?.length && Array.isArray(data.matchedCustomEvents) ? data.matchedCustomEvents.map(event => {
                        const customEvent = lexiconStore.findCustomEventById(Number(event.id))

                        return {
                            name: customEvent.name,
                            value: Number(event.id)
                        }
                    }) : [],
                    component: componentsMaps[column.value] || null,
                }

                if (column.value === eventName) {
                    cell.action = {
                        type: eventName,
                        name: data.name,
                    }
                }

                return cell
            } else if (column.value === actionTable) {
                return {
                    title: actionTable,
                    key: actionTable,
                    value: actionTable,
                    component: UiCellToolMenu,
                    items: [
                        {
                            label: i18n.$t('events.create_custom'),
                            value: createCustomEvent,
                        },
                    ],
                    type: actionTable
                }
            } else {
                const value = column.value in data.properties ? data.properties[column.value] : data.userProperties && column.value in data.userProperties ? data.userProperties[column.value] : ''

                return {
                    key: column.value,
                    title: value || '-',
                    nowrap: true,
                }
            }
        })
    })
})

const calendarValue = computed(() => {
    return {
        from: liveStreamStore.period?.from,
        to: liveStreamStore.period?.to,
        multiple: false,
        dates: [],
    }
})

const columnsButtonText = computed(() => {
    return `${liveStreamStore.columnsMap.length} ${i18n.$t('common.columns')}`
})

const selectColumns = computed((): Group<Item<string, null>[]>[] => {
    return [
        {
            name: i18n.$t(`events.live_stream.popupTabs.${properties}`),
            items: liveStreamStore.columnsMapObject.properties.map(item => {
                return {
                    item: item,
                    name: item.charAt(0).toUpperCase() + item.slice(1),
                    selected: liveStreamStore.activeColumns.includes(item)
                }
            })
        },
        {
            name: i18n.$t(`events.live_stream.popupTabs.${userProperties}`),
            items: liveStreamStore.columnsMapObject.userProperties.map(item => {
                return {
                    name: item.charAt(0).toUpperCase() + item.slice(1),
                    item: item,
                    selected: liveStreamStore.activeColumns.includes(item)
                }
            })
        }
    ]
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

const updateReport = () => {
    liveStreamStore.getReportLiveStream()
}

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
    if (payload.name === createCustomEvent) {
        eventsStore.setEditCustomEvent(null)
        commonStore.togglePopupCreateCustomEvent(true)
    }

    if (payload.type === 'event') {
        eventsStore.setEditCustomEvent(Number(payload.name))
        commonStore.togglePopupCreateCustomEvent(true)
    }

    if (payload.type === eventName) {
        eventPopupName.value = payload.name
        liveStreamStore.eventPopup = true
    }
}
</script>

<style scoped lang="scss">
</style>
