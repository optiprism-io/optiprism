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
                v-if="activeTab === 'property'"
                :items="propertyItems"
                :horizontal="true"
                @on-input="onInputPropertyItem"
            />
            <UiTable
                v-if="activeTab === 'events'"
                :items="itemsEvents"
                :columns="itemsEventsColumns"
            />
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { Property, Event } from '@/api'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiTabs from '@/components/uikit/UiTabs.vue'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'
import UiDescriptionList, { Item, ActionPayload } from '@/components/uikit/UiDescriptionList.vue'
import { Row } from '@/components/uikit/UiTable/UiTable'
import { propertyValuesConfig, Item as PropertyValueConfig } from '@/configs/events/eventValues'

export type EventObject = {
    [key: string]: string | string[] | boolean
}
export type ApplyPayload = EventObject

const mapTabs = ['property', 'events']

const i18n = inject<any>('i18n')

type Props = {
    name?: string
    loading?: boolean
    events?: Event[] | null
    property: Property | null
}

const props = withDefaults(defineProps<Props>(), {
    name: '',
})

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply', payload: ApplyPayload): void
}>()

const activeTab = ref('property')

const editProperty = ref<EventObject | null>(null)
const applyDisabled = computed(() => !editProperty.value)

const itemsTabs = computed(() => {
    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.event_management.popup.tabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
})

const title = computed(() => props.property ? `${i18n.$t('events.event_management.popup.tabs.property')}: ${props.property.name}` : '')

const propertyItems = computed<Item[]>(() => {
    const items: Item[] = [];
    const property = props.property;
    if (property) {
        const keys = (Object.keys(propertyValuesConfig) as (keyof typeof props.property)[])

        keys.forEach(key => {
            const config: PropertyValueConfig = propertyValuesConfig[key];
            let value = editProperty.value && key in editProperty.value ? editProperty.value[key] : property[key] ||property[key];

            if (key === 'status') {
                value = property[key] === 'enabled'
            }

            if (key === 'type') {
                value = property.isArray ? i18n.$t('common.list_of', { type: i18n.$t(`common.types.${property.dataType}`) }) : i18n.$t(`common.types.${property.dataType}`)
            }

            if (config.key in property) {
                const item: Item = {
                    label: i18n.$t(config.string),
                    key,
                    value,
                    component: config.component || 'p'
                }

                items.push(item)
            }
        })
    }

    return items
})

const itemsEventsColumns = computed(() => {
    return ['name', 'displayName', 'isSystem'].map(key => {
        return {
            value: key,
            title: i18n.$t(`events.event_management.columns.${key}`),
        }
    })
})

const itemsEvents = computed(() => {
    if (props.events) {
        return props.events.map((event: Event): Row => {
            return [
                {
                    value: 'name',
                    title: event.name,
                    component: UiTablePressedCell,
                    action: {
                        type: event.id,
                        name: event.name,
                    }
                },
                {
                    value: 'displayName',
                    title: event.displayName || '',
                    nowrap: true,
                },
                {
                    value: 'isSystem',
                    title: String(event.isSystem),
                }
            ]
        })
    } else {
        return []
    }
})

const onSelectTab = (payload: string) => {
    activeTab.value = payload
}

const apply = () => {
    if (editProperty.value) {
        emit('apply', editProperty.value)
    }
}

const cancel = () => {
    emit('cancel')
}

const onInputPropertyItem = async (payload: ActionPayload) => {
    let value = payload.value

    if (payload.key === 'status') {
        value = payload.value ? 'enabled' : 'disabled'
    }

    if (editProperty.value) {
        editProperty.value[payload.key] = value
    } else {
        editProperty.value = {
            [payload.key]: value
        }
    }
}
</script>