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
                    :show-select-columns="true"
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
import UiCellTags from '@/components/uikit/cells/UiCellTags.vue'
import UiCellToolMenu from '@/components/uikit/cells/UiCellToolMenu.vue'
const i18n = inject<any>('i18n')
const lexiconStore = useLexiconStore()
const commonStore = useCommonStore()

const columns = computed(() => {
    return ['name', 'displayName', 'description', 'tags', 'status', 'action'].map(key => {
        const isAction = key === 'action'

        return {
            value: key,
            title: isAction ? '' : i18n.$t(`events.event_management.columns.${key}`),
            default: isAction,
            type: isAction? 'action' : '',
        }
    })
})

const items = computed(() => {
    return lexiconStore.events.map((event: Event): Row => {
        return [
            {
                key: 'name',
                value: 'name',
                title: event.name,
                component: UiTablePressedCell,
                action: {
                    type: event.id,
                    name: event.name,
                }
            },
            {
                key: 'displayName',
                title: event.displayName || '',
                nowrap: true,
            },
            {
                key: 'description',
                title: event.description || '',
            },
            {
                key: 'tags',
                title: 'tags',
                value: event.tags || [],
                nowrap: Boolean(event.tags?.length || 0 <= 5),
                component: UiCellTags,
            },
            {
                key: 'status',
                title: event.status || '',
            },
            {
                title: 'action',
                key: 'action',
                value: event.id,
                component: UiCellToolMenu,
                items: [
                    {
                        label: i18n.$t('events.editEvent'),
                        value: 'edit',
                    },
                ],
                type: 'action'
            }
        ]
    })
})

const onAction = (payload: Action) => {
    commonStore.updateEditEventManagementPopupId(Number(payload.type) || null)
    commonStore.toggleEventManagementPopup(true)
}
</script>