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
    <EventManagementPopup
        v-if="commonStore.showEventManagementPopup"
        :event="editEventManagementPopup"
        :properties="eventProperties"
        :user-properties="userProperties"
        @update-event="updateEvent"
        @apply="eventManagementPopupApply"
        @cancel="eventManagementPopupCancel"
    />
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import { Row, Action } from '@/components/uikit/UiTable/UiTable'
import { Event } from '@/types/events'
import schemaService from '@/api/services/schema.service'
import { UpdateEventRequest } from '@/api'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTableCells/UiTablePressedCell.vue'
import EventManagementPopup from '@/components/events/EventManagementPopup.vue'
import { ActionPayload } from '@/components/uikit/UiDescriptionList.vue'
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
            },
            {
                value: 'description',
                title: event.description || '',
            }
        ]
    })
})

const editEventManagementPopup = computed(() => {
    if (commonStore.editEventManagementPopupId) {
        return lexiconStore.findEventById(commonStore.editEventManagementPopupId)
    } else {
        return null
    }
})

const eventProperties = computed(() => {
    return editEventManagementPopup.value && editEventManagementPopup.value?.event_properties ?
        editEventManagementPopup.value?.event_properties.map(id => lexiconStore.findEventPropertyById(id)) : []
})

const userProperties = computed(() => {
    return editEventManagementPopup.value && editEventManagementPopup.value?.user_properties ?
        editEventManagementPopup.value?.user_properties.map(id => lexiconStore.findUserPropertyById(id)) : []
})

const onAction = (payload: Action) => {
    commonStore.updateEditEventManagementPopupId(Number(payload.type) || null)
    commonStore.toggleEventManagementPopup(true)
}

const eventManagementPopupApply = () => {
    commonStore.toggleEventManagementPopup(false)
}

const eventManagementPopupCancel = () => {
    commonStore.toggleEventManagementPopup(false)
}

const updateEvent = async (payload: ActionPayload) => {
    const requestKeysMap = ['status', 'displayName', 'description', 'tags']

    const requestData: UpdateEventRequest = {
        [payload.key]: payload.value
    }

    if (requestKeysMap.includes(payload.key)) {
        if (payload.key === 'status') {
            requestData.status = payload.value ? 'enabled' : 'disabled'
        }
    }

    await schemaService.updateEvent(String(commonStore.projectId), String(commonStore.editEventManagementPopupId), requestData)
}
</script>

<style scoped lang="scss">
</style>
