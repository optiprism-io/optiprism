<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="title"
        :apply-loading="loading"
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
    return props.event ? (Object.keys(props.event) as (keyof typeof props.event)[]).map(key => {
        return {
            label: key,
            text:  props.event ? JSON.stringify(props.event[key]) : '',
            type: 'text'
        }
    }) : []
})

const title = computed(() => `${i18n.$t('events.event_management.event')}: ${props.name}`)

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