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
        :loading="eventManagementPopupLoading"
        @apply="eventManagementPopupApply"
        @cancel="eventManagementPopupCancel"
    />
</template>

<script setup lang="ts">
import { computed, inject, ref } from 'vue'
import { useLexiconStore } from '@/stores/lexicon'
import { useCommonStore } from '@/stores/common'
import UiTable from '@/components/uikit/UiTable/UiTable.vue'
import { Row, Action } from '@/components/uikit/UiTable/UiTable'
import { Event } from '@/types/events'
import schemaService from '@/api/services/schema.service'
import UiTablePressedCell from '@/components/uikit/UiTable/UiTableCells/UiTablePressedCell.vue'
import EventManagementPopup, { ApplyPayload } from '@/components/events/EventManagementPopup.vue'
const i18n = inject<any>('i18n')
const lexiconStore = useLexiconStore()
const commonStore = useCommonStore()

const eventManagementPopupLoading = ref(false)

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

const updateEvent = async (payload: ApplyPayload) => {
    await schemaService.updateEvent(String(commonStore.projectId), String(commonStore.editEventManagementPopupId), payload)
}

const eventManagementPopupApply = async (payload: ApplyPayload) => {
    eventManagementPopupLoading.value = true
    await updateEvent(payload)
    eventManagementPopupLoading.value = false
}

const eventManagementPopupCancel = () => {
    commonStore.toggleEventManagementPopup(false)
}

</script>

<style scoped lang="scss">
</style>
