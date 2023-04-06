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
import { useCommonStore, PropertyTypeEnum } from '@/stores/common'
import EventPropertyPopup, { ApplyPayload } from '@/components/events/EventPropertyPopup.vue'
import EventManagementPopup, { ApplyPayload as ApplyPayloadEvent } from '@/components/events/EventManagementPopup.vue'
import { Action } from '@/components/uikit/UiTable/UiTable'
import navPagesConfig from '@/configs/events/navPages.json'
import { pagesMap } from '@/router'
const i18n = inject<any>('i18n')
const route = useRoute()
const lexiconStore = useLexiconStore()
const liveStreamStore = useLiveStreamStore()
const commonStore = useCommonStore()

const propertyPopupLoading = ref(false)
const eventManagementPopupLoading = ref(false)

const items = computed(() => {
    return navPagesConfig.map(item => {
        return {
            ...item,
            name: i18n.$t(item.name),
            active: route.name === item.value,
        }
    })
})

const editPropertyPopup = computed(() => {
    if (commonStore.editEventPropertyPopupId) {
        if (commonStore.editEventPropertyPopupType === PropertyTypeEnum.EventProperty) {
            return lexiconStore.findEventPropertyById(commonStore.editEventPropertyPopupId)
        } else {
            return lexiconStore.findUserPropertyById(commonStore.editEventPropertyPopupId)
        }
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
    return editEventManagementPopup.value && editEventManagementPopup.value?.eventProperties ?
        editEventManagementPopup.value.eventProperties.map(id => lexiconStore.findEventPropertyById(id)) : []
})

const userProperties = computed(() => {
    return editEventManagementPopup.value && editEventManagementPopup.value?.userProperties ?
        editEventManagementPopup.value?.userProperties.map(id => lexiconStore.findUserPropertyById(id)) : []
})

onMounted(async () => {
    liveStreamStore.getReportLiveStream()
});

const propertyPopupApply = async (payload: ApplyPayload) => {
    propertyPopupLoading.value = true
    await lexiconStore.updateProperty(payload)
    propertyPopupLoading.value = false
    commonStore.showEventPropertyPopup = false

    if (route.name === pagesMap.eventsLiveStream.name) {
        liveStreamStore.getReportLiveStream()
    }
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
