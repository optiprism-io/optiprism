<template>
    <ToolsLayout>
        <template #title>
            {{ $t('events.event_properties') }}
        </template>
        <template #main>
            <UiCardContainer class="pf-u-h-100">
                <UiTable
                    :items="items"
                    :columns="columns"
                    :show-select-columns="true"
                    @on-action="onAction"
                />
            </UiCardContainer>
        </template>
    </ToolsLayout>
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'
import { Property } from '@/api'
import { Action, Row }  from '@/components/uikit/UiTable/UiTable'

import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'
import UiCellTags from '@/components/uikit/cells/UiCellTags.vue'
import UiCellToolMenu from '@/components/uikit/cells/UiCellToolMenu.vue'
import ToolsLayout from '@/layout/tools/ToolsLayout.vue'
import UiCardContainer from '@/components/uikit/UiCard/UiCardContainer.vue'

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
    return lexiconStore.eventProperties.map((property: Property): Row => {
        return [
            {
                key: 'name',
                title: property.name,
                component: UiTablePressedCell,
                action: {
                    type: property.id,
                    name: property.name,
                }
            },
            {
                key: 'displayName',
                title: property.displayName || '',
                nowrap: true,
            },
            {
                key: 'description',
                title: property.description || '',
            },
            {
                key: 'tags',
                title: 'tags',
                value: property.tags || [],
                nowrap: Boolean(property.tags?.length || 0 <= 5),
                component: UiCellTags,
            },
            {
                key: 'status',
                title: property.status,
            },
            {
                title: 'action',
                key: 'action',
                value: property.id,
                component: UiCellToolMenu,
                items: [
                    {
                        label: i18n.$t('events.editProperty'),
                        value: 'edit',
                    },
                ],
                type: 'action'
            }
        ]
    })
})

const onAction = (payload: Action) => {
    commonStore.editEventPropertyPopupId = Number(payload.type) || null
    commonStore.showEventPropertyPopup = true
}
</script>