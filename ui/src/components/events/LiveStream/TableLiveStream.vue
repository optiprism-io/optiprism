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
                                            'pf-m-selected': isPeriodActive,
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
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { computed, inject } from 'vue'
import { useLiveStreamStore } from '@/stores/reports/liveStream'
import { getStringDateByFormat } from '@/helpers/getStringDates'

import { ApplyPayload } from '@/components/uikit/UiCalendar/UiCalendar'
import UiToggleGroup, { UiToggleGroupItem } from '@/components/uikit/UiToggleGroup.vue'
import UiDatePicker from '@/components/uikit/UiDatePicker.vue'

const i18n = inject<any>('i18n')
const liveStreamStore = useLiveStreamStore()

const itemsPeriod = computed(() => {
    return ['7', '30', '90'].map((key, i): UiToggleGroupItem => ({
        key,
        nameDisplay: key + i18n.$t('common.calendar.day_short'),
        value: key,
        selected: liveStreamStore.controlsPeriod === key,
    }))
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

const isPeriodActive = computed(() => liveStreamStore.period.from && liveStreamStore.period.to && liveStreamStore.controlsPeriod === 'calendar')

const calendarValueString = computed(() => {
    if (isPeriodActive.value) {
        switch(liveStreamStore.period.type) {
            case 'last':
                return `Last ${liveStreamStore.period.last} ${liveStreamStore.period.last === 1 ? 'day' : 'days'}`
            case 'since':
                return `Since ${getStringDateByFormat(liveStreamStore.period.from, '%d %b, %Y')}`
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
}
</script>

<style scoped lang="scss">
</style>