<template>
    <UiPopupWindow
        :apply-button="$t('common.apply')"
        :cancel-button="$t('common.cancel')"
        :title="$t('events.create_custom')"
        :description="$t('events.create_custom_description')"
        :apply-disabled="applyDisabled"
        @apply="apply"
        @cancel="cancel"
    >
        <UiFormLabel
            class="pf-u-mb-lg"
            :text="$t('events.custom_event_name')"
            :required="true"
            :for="'eventName'"
        >
            <UiInput
                v-model="eventName"
                :required="true"
                :name="'eventName'"
                :label="$t('events.custom_event_name')"
            />
        </UiFormLabel>
        <div class="pf-u-font-size-md pf-u-font-weight-bold pf-u-mb-md">
            {{ $t('events.events') }}
        </div>
        <SelectedEvent
            v-for="(event, index) in events"
            :key="index"
            :event="event"
            :event-ref="event.ref"
            :filters="event.filters"
            :index="index"
            :event-items="eventItems"
            :show-breakdowns="false"
            :show-query="false"
            @set-event="setEvent"
            @remove-event="removeEvent"
        />
        <Select
            grouped
            :items="eventItems"
            :width-auto="true"
            :popper-class="'popup-floating-popper'"
            @select="addEvent"
        >
            <UiButton
                class="pf-m-main"
                :is-link="true"
                :before-icon="'fas fa-plus'"
            >
                {{ $t('common.add_event') }}
            </UiButton>
        </Select>
    </UiPopupWindow>
</template>

<script lang="ts" setup>
import { ref, computed } from 'vue'
import { EventRef, EVENT_TYPE_REGULAR } from '@/types/events'
import { useLexiconStore } from '@/stores/lexicon'
import { Event } from '@/stores/eventSegmentation/events'

import UiPopupWindow from '@/components/uikit/UiPopupWindow.vue'
import UiInput from '@/components/uikit/UiInput.vue'
import UiFormLabel from '@/components//uikit/UiFormLabel.vue'
import Select from '@/components/Select/Select.vue'
import SelectedEvent, { SetEventPayload } from '@/components/events/Events/SelectedEvent.vue'

const lexiconStore = useLexiconStore()

const emit = defineEmits<{
    (e: 'cancel'): void
    (e: 'apply'): void
}>()

const eventName = ref('')

const events = ref<Event[]>([])

const applyDisabled = computed(() => {
    return !events.value.length
})

const eventItems = computed(() => {
    return lexiconStore.eventsList.filter((group) => !group.type && group.type !== 'custom')
})

const addEvent = (ref: EventRef) => {
    events.value.push(<Event>{
        ref: <EventRef>{
            type: EVENT_TYPE_REGULAR,
            id: ref.id
        },
        filters: [],
        breakdowns: [],
        queries: [],
    })
}

const setEvent = (payload: SetEventPayload) => {
    events.value[payload.index] = payload.event
}

const removeEvent = (idx: number): void => {
    events.value.splice(idx, 1)
}

const apply = () => {
    emit('apply')
}

const cancel = (type: string) => {
    emit('cancel')
}
</script>

<style lang="scss">
</style>