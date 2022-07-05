<template>
    <h1 class="pf-u-font-size-2xl pf-u-mb-md">
        {{ $t('events.event_properties') }}
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
    <EventPropertyPopup
        v-if="commonStore.showEventPropertyPopup"
        :loading="propertyPopupLoading"
        :property="editPropertyPopup"
        @apply="propertyPopupApply"
        @cancel="propertyPopupCancel"
    />
</template>

<script setup lang="ts">
import {computed, inject, ref} from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'
import { Property } from '@/api'
import { Action, Row }  from '@/components/uikit/UiTable/UiTable'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTablePressedCell.vue'
import EventPropertyPopup, { ApplyPayload } from '@/components/events/EventPropertyPopup.vue'

const i18n = inject<any>('i18n')
const lexiconStore = useLexiconStore()
const commonStore = useCommonStore()

const propertyPopupLoading = ref(false)

const columns = computed(() => {
    return ['name', 'displayName', 'description', 'status'].map(key => {
        return {
            value: key,
            title: i18n.$t(`events.event_management.columns.${key}`),
        }
    })
})

const items = computed(() => {
    return lexiconStore.eventProperties.map((property: Property): Row => {
        return [
            {
                value: 'name',
                title: property.name,
                component: UiTablePressedCell,
                action: {
                    type: property.id,
                    name: property.name,
                }
            },
            {
                value: 'displayName',
                title: property.displayName || '',
                nowrap: true,
            },
            {
                value: 'description',
                title: property.description || '',
            },
            {
                value: 'status',
                title: property.status,
            }
        ]
    })
})

const editPropertyPopup = computed(() => {
    if (commonStore.editEventPropertyPopupId) {
        return lexiconStore.findEventPropertyById(commonStore.editEventPropertyPopupId)
    } else {
        return null
    }
})

const onAction = (payload: Action) => {
    commonStore.editEventPropertyPopupId = Number(payload.type) || null
    commonStore.showEventPropertyPopup = true
}

const propertyPopupApply = async (payload: ApplyPayload) => {
    propertyPopupLoading.value = true
    // TODO edit property save to store and send backend
    propertyPopupLoading.value = false
    commonStore.showEventPropertyPopup = false
}

const propertyPopupCancel = () => {
    commonStore.showEventPropertyPopup = false
}
</script>