<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="event-management-popup"
        :apply-button="$t('common.save')"
        :cancel-button="$t('common.close')"
        :apply-disabled="applyDisabled"
        @apply="apply"
        @cancel="cancel"
    >
        <UiTabs
            class="pf-u-mb-md"
            :items="itemsTabs"
            @on-select="onSelectTab"
        />
        <div class="event-management-popup__content">
            <UiDescriptionList
                v-if="activeTab === 'event'"
                :items="eventItems"
                :horizontal="true"
                @on-input="onInputEventItem"
            />
            <UiTable
                v-if="activeTab === 'properties'"
                :compact="true"
                :items="itemsProperties"
                :columns="columnsProperties"
            />
            <UiTable
                v-if="activeTab === 'custom_properties'"
                :compact="true"
                :items="itemsUserProperties"
                :columns="columnsProperties"
            />
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { EventCustomProperty } from '@/types/events'
import { Row } from '@/components/uikit/UiTable/UiTable'
import { Property, Event } from '@/api'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiTabs from '@/components/uikit/UiTabs.vue'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import UiDescriptionList, { Item, ActionPayload } from '@/components/uikit/UiDescriptionList.vue'

import propertiesColumnsConfig from '@/configs/events/propertiesTable.json'
import { eventValuesConfig, Item as EventValuesConfig } from '@/configs/events/eventValues'

export type EventObject = {
    [key: string]: string | string[] | boolean
}
export type ApplyPayload = EventObject

const mapTabs = ['event', 'properties', 'custom_properties']

const i18n = inject<any>('i18n')

type Props = {
    name?: string
    loading?: boolean
    event: Event | null
    properties: Property[]
    userProperties: EventCustomProperty[]
}

const props = withDefaults(defineProps<Props>(), {
    name: '',
})

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply', payload: ApplyPayload): void
}>()

const activeTab = ref('event')

const editEvent = ref<EventObject | null>(null)
const applyDisabled = computed(() => !editEvent.value)

const getTableRows = (properties: Property[] | EventCustomProperty[]) => {
    return properties.map((prop: Property | EventCustomProperty): Row => {
        const keys = (propertiesColumnsConfig.map(item => item.key) as (keyof typeof prop)[])
        const rows: Row = [];

        keys.forEach((key) => {
            if (prop[key]) {
                rows.push({
                    value: key,
                    title: String(prop[key]) || '',
                })
            }
        })

        return rows
    })
}

const itemsTabs = computed(() => {
    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.event_management.popup.tabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
})

const eventItems = computed<Item[]>(() => {
    const items: Item[] = [];
    const event = props.event;

    if (event) {
        const keys = (Object.keys(eventValuesConfig) as (keyof typeof props.event)[])

        keys.forEach(key => {
            const config: EventValuesConfig = eventValuesConfig[key];

            if (key in event) {
                const item: Item = {
                    label: i18n.$t(config.string),
                    key,
                    value: key === 'status' ? event[key] === 'enabled' : editEvent.value && key in editEvent.value ? editEvent.value[key] : event[key],
                    component: config.component || 'p'
                }

                items.push(item)
            }
        })
    }

    return items
})

const title = computed(() => props.event ? `${i18n.$t('events.event_management.event')}: ${props.event.name}` : '')

const columnsProperties = computed(() => {
    return propertiesColumnsConfig.map(item => {
        return {
            value: item.key,
            title: i18n.$t(item.string),
        }
    })
})

const itemsProperties = computed(() => getTableRows(props.properties))
const itemsUserProperties = computed(() => getTableRows(props.userProperties))

const onSelectTab = (payload: string) => {
    activeTab.value = payload
}

const apply = () => {
    if (editEvent.value) {
        emit('apply', editEvent.value)
    }
}

const cancel = () => {
    emit('cancel')
}

const onInputEventItem = async (payload: ActionPayload) => {
    let value = payload.value

    if (payload.key === 'status') {
        value = payload.value ? 'enabled' : 'disabled'
    }

    if (editEvent.value) {
        editEvent.value[payload.key] = value
    } else {
        editEvent.value = {
            [payload.key]: value
        }
    }
}
</script>