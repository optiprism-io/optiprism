<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
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
        <UiDescriptionList
            v-if="activeTab === 'event'"
            :items="eventItems"
            :horizontal="true"
        />
        <div v-else>
            TODO
        </div>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { computed, inject, ref } from 'vue'
import { Event } from '@/types/events'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiTabs from '@/components/uikit/UiTabs.vue'
import UiDescriptionList, { Item } from '@/components/uikit/UiDescriptionList.vue'
import { getStringDateByFormat } from '@/helpers/getStringDates';

const i18n = inject<any>('i18n')

type Props = {
    name?: string
    loading?: boolean
    event: Event | null
}

const props = withDefaults(defineProps<Props>(), {
    name: '',
})

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const activeTab = ref('event')

const itemsTabs = computed(() => {
    const mapTabs = ['event', 'properties']

    return mapTabs.map(key => {
        return {
            name: i18n.$t(`events.event_management.popup.tabs.${key}`),
            active: activeTab.value === key,
            value: key,
        }
    })
})

const eventItems = computed<Item[]>(() => {
    const hiddenKeys = ['id', 'properties', 'createdBy', 'updatedBy', 'projectId'];
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

<style lang="scss"></style>