<template>
    <UiPopupWindow
        :title="title"
        :apply-loading="props.loading"
        class="event-management-popup"
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
            />
            <UiTable
                v-if="activeTab === 'properties'"
                :compact="true"
                :items="itemsProperties"
                :columns="columnsProperties"
            />
            <UiTable
                v-if="activeTab === 'user_properties'"
                :compact="true"
                :items="itemsUserProperties"
                :columns="columnsProperties"
            />
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { Event, UserProperty } from '@/types/events'
import { Row } from '@/components/uikit/UiTable/UiTable'
import { Property } from '@/api'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiTabs from '@/components/uikit/UiTabs.vue'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import UiDescriptionList, { Item } from '@/components/uikit/UiDescriptionList.vue'

import { getStringDateByFormat } from '@/helpers/getStringDates'
import propertiesColumnsConfig from '@/configs/events/propertiesTable.json'
const mapTabs = ['event', 'properties', 'user_properties']

const i18n = inject<any>('i18n')

type Props = {
    name?: string
    loading?: boolean
    event: Event | null
    properties: Property[]
    userProperties: UserProperty[]
}

const props = withDefaults(defineProps<Props>(), {
    name: '',
})

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const activeTab = ref('event')

const getTableRows = (properties: Property[] | UserProperty[]) => {
    return properties.map((prop: Property | UserProperty): Row => {
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
    const hiddenKeys = ['id', 'properties', 'createdBy', 'updatedBy', 'projectId', 'event_properties', 'user_properties'];
    const items: Item[] = [];
    const event = props.event;

    if (event) {
        const keys = (Object.keys(props.event) as (keyof typeof props.event)[]).filter(key => !hiddenKeys.includes(key))

        keys.forEach(key => {
            if (event[key]) {
                const item: Item = {
                    label: key,
                    text: '',
                    type: 'text'
                }

                switch (key) {
                    case 'createdAt':
                        item.text = getStringDateByFormat(event[key], '%d %b, %Y')
                        break
                    case 'tags':
                        item.text = event[key]
                        item.type = 'label'
                        break
                    default:
                        item.text = event[key]
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
    emit('apply')
}

const cancel = () => {
    emit('cancel')
}
</script>

<style lang="scss">
.event-management-popup {
    &__content {
        min-height: 15rem;
    }
}
</style>