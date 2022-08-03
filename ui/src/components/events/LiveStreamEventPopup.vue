<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="live-stream-event-popup"
        @apply="apply"
        @cancel="cancel"
    >
        <UiTabs
            class="pf-u-mb-md"
            :items="itemsTabs"
            @on-select="onSelectTab"
        />
        <div class="live-stream-event-popup__content">
            <UiTable
                :compact="true"
                :items="items"
                :columns="columns"
            />
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { useLiveStreamStore, Report } from '@/stores/reports/liveStream'

import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import { Row } from '@/components/uikit/UiTable/UiTable'
import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import { getStringDateByFormat } from '@/helpers/getStringDates'
const i18n = inject<any>('i18n')
const liveStreamStore = useLiveStreamStore()

type Props = {
    name: string
    loading?: boolean
}

const properties = 'properties'
const createdAt = 'createdAt'

const mapTabs = [properties, 'userProperties']

const props = defineProps<Props>()

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const activeTab = ref(properties)

const report = computed(() => {
    return liveStreamStore.reports.find((item: Report) => item.name === props.name)
})

const items = computed(() => {
    const objItems = report.value[activeTab.value]

    if (objItems) {
        return Object.keys(objItems).map((key): Row => {
            return [
                {
                    key: 'name',
                    title: key === createdAt ? i18n.$t('events.live_stream.columns.createdAt') : activeTab.value === properties ? key.charAt(0).toUpperCase() + key.slice(1) : key
                },
                {
                    key: 'value',
                    title: key === createdAt ? getStringDateByFormat(String(objItems[key]), '%d %b, %Y') : objItems[key],
                }
            ]
        })
    } else {
        return []
    }
})

const columns = computed(() => {
    return ['name', 'value'].map(key => {
        return {
            value: key,
            title: i18n.$t(`events.event_management.columns.${key}`),
        }
    })
})

const title = computed(() => {
    return `${i18n.$t('events.event_management.event')}: ${props.name}`
})

const itemsTabs = computed(() => {
    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.live_stream.popupTabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
})

const onSelectTab = (payload: string) => {
    activeTab.value = payload
}

const apply = () => {
    emit('apply')
}

const cancel = () => {
    emit('cancel')
    liveStreamStore.eventPopup = false
}

</script>
