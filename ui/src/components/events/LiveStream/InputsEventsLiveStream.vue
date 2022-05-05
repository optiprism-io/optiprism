<template>
    <div class="pf-l-flex pf-m-column">
        <SelectedEvent
            v-for="(event, index) in events"
            :key="index"
            :event="event"
            :event-ref="event.ref"
            :filters="event.filters"
            :index="index"
            :event-items="lexiconStore.eventsList"
            :show-breakdowns="false"
            :show-query="true"
            @set-event="setEvent"
            @remove-event="removeEvent"
        />
    </div>
    <Select
        grouped
        :items="lexiconStore.eventsList"
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
</template>

<script lang="ts" setup>
import { ref, computed } from 'vue'
import { EventRef } from '@/types/events'
import { Event, EventPayload } from '@/stores/eventSegmentation/events'

import { useLexiconStore } from '@/stores/lexicon'

import Select from '@/components/Select/Select.vue'
import SelectedEvent from '@/components/events/Events/SelectedEvent.vue'


const lexiconStore = useLexiconStore()

const events = ref<Event[]>([])

const addEvent = (ref: EventRef) => {
    events.value.push({
        ref: {
            type: ref.type,
            id: ref.id
        },
        filters: [],
        breakdowns: [],
        queries: [],
    })
}

const setEvent = (payload: EventPayload) => {
    events.value[payload.index] = payload.event
}

const removeEvent = (idx: number): void => {
    events.value.splice(idx, 1)
}
</script>

<style scoped lang="scss">

</style>
