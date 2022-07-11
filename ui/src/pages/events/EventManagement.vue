<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.events') }}
    </h1>
    <div class="pf-l-grid pf-m-gutter">
        <div class="pf-l-grid__item">
            <div class="pf-c-card pf-m-compact pf-u-h-100">
                <UiTable
                    :items="items"
                    :columns="columns"
                    @on-action="onAction"
                />
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import { Row, Action } from '@/components/uikit/UiTable/UiTable'
import { Event } from '@/api'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'
const i18n = inject<any>('i18n')
const lexiconStore = useLexiconStore()
const commonStore = useCommonStore()

const columns = computed(() => {
    return ['name', 'displayName', 'description'].map(key => {
        return {
            value: key,
            title: i18n.$t(`events.event_management.columns.${key}`),
        }
    })
})

const items = computed(() => {
    return lexiconStore.events.map((event: Event): Row => {
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
                value: 'description',
                title: event.description || '',
            }
        ]
    })
})

const onAction = (payload: Action) => {
    commonStore.updateEditEventManagementPopupId(Number(payload.type) || null)
    commonStore.toggleEventManagementPopup(true)
}
</script>