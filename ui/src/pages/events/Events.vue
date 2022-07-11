<template>
    <section class="pf-c-page__main-section">
        <UiTabs
            class="pf-u-mb-md"
            :items="items"
        />
        <router-view />

        <EventPropertyPopup
            v-if="commonStore.showEventPropertyPopup"
            :loading="propertyPopupLoading"
            :property="editPropertyPopup"
            :events="editPropertyEventsPopup"
            @apply="propertyPopupApply"
            @cancel="propertyPopupCancel"
            @on-action-event="onActionEvent"
        />
        <EventManagementPopup
            v-if="commonStore.showEventManagementPopup"
            :event="editEventManagementPopup"
            :properties="eventProperties"
            :user-properties="userProperties"
            :loading="eventManagementPopupLoading"
            @apply="eventManagementPopupApply"
            @cancel="eventManagementPopupCancel"
            @on-action-property="onActionProperty"
        />
    </section>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { useLexiconStore } from '@/stores/lexicon'
import { useLiveStreamStore } from '@/stores/reports/liveStream'
import EventPropertyPopup, { ApplyPayload } from '@/components/events/EventPropertyPopup.vue'
import EventManagementPopup, { ApplyPayload as ApplyPayloadEvent } from '@/components/events/EventManagementPopup.vue'
import { Action } from '@/components/uikit/UiTable/UiTable'
import { useCommonStore } from '@/stores/common'

const i18n = inject<any>('i18n')
const route = useRoute()
const lexiconStore = useLexiconStore()
const liveStreamStore = useLiveStreamStore()
const commonStore = useCommonStore()

const propertyPopupLoading = ref(false)
const eventManagementPopupLoading = ref(false)

const items = computed(() => {
    const mapTabs = [
        {
            name: i18n.$t('events.live_stream.title'),
            value: 'events_live_stream',
            link: {
                name: 'events_live_stream',
            },
            icon: 'fas fa-chart-pie'
        },
        {
            name: i18n.$t('events.events'),
            value: 'events_event_management',
            link: {
                name: 'events_event_management',
            },
            icon: 'pf-icon pf-icon-filter'
        },
        {
            name: i18n.$t('events.event_properties'),
            value: 'events_event_properties',
            link: {
                name: 'events_event_properties',
            },
            icon: 'fas fa-bars'
        },
    ];

    return mapTabs.map(item => {
        return {
            ...item,
            active: route.name === item.value,
        }
    })
})

const editPropertyPopup = computed(() => {
    if (commonStore.editEventPropertyPopupId) {
        return lexiconStore.findEventPropertyById(commonStore.editEventPropertyPopupId)
    } else {
        return null
    }
})

const editPropertyEventsPopup = computed(() => {
    if (commonStore.editEventPropertyPopupId) {
        const property = lexiconStore.findEventPropertyById(commonStore.editEventPropertyPopupId)
        if (property.events?.length) {
            return property.events.map(id => {
                return lexiconStore.findEventById(id)
            })
        } else {
            return []
        }
    } else {
        return []
    }
})


const eventManagementPopupCancel = () => {
    commonStore.toggleEventManagementPopup(false)
}

const eventManagementPopupApply = async (payload: ApplyPayloadEvent) => {
    eventManagementPopupLoading.value = true
    await lexiconStore.updateEvent(payload)
    eventManagementPopupLoading.value = false
    commonStore.toggleEventManagementPopup(false)
}

const editEventManagementPopup = computed(() => {
    if (commonStore.editEventManagementPopupId) {
        return lexiconStore.findEventById(commonStore.editEventManagementPopupId)
    } else {
        return null
    }
})

const eventProperties = computed(() => {
    return editEventManagementPopup.value && editEventManagementPopup.value?.properties ?
        editEventManagementPopup.value.properties.map(id => lexiconStore.findEventPropertyById(id)) : []
})

const userProperties = computed(() => {
    return editEventManagementPopup.value && editEventManagementPopup.value?.custom_properties ?
        editEventManagementPopup.value.custom_properties.map(id => lexiconStore.findEventCustomPropertyById(id)) : []
})

onMounted(async () => {
    await liveStreamStore.getReportLiveStream()
    await lexiconStore.getEvents()
    await lexiconStore.getEventProperties()
    await lexiconStore.getUserProperties()
})

const propertyPopupApply = async (payload: ApplyPayload) => {
    propertyPopupLoading.value = true
    await lexiconStore.updateProperty(payload)
    propertyPopupLoading.value = false
    commonStore.showEventPropertyPopup = false
}

const propertyPopupCancel = () => {
    commonStore.showEventPropertyPopup = false
}

const onActionEvent = (payload: Action) => {
    commonStore.updateEditEventManagementPopupId(Number(payload.type) || null)
    commonStore.showEventPropertyPopup = false
    commonStore.showEventManagementPopup = true
}

const onActionProperty = (payload: Action) => {
    commonStore.editEventPropertyPopupId = Number(payload.type) || null
    commonStore.showEventManagementPopup = false
    commonStore.showEventPropertyPopup = true
}
</script>

<style scoped lang="scss">
</style>
